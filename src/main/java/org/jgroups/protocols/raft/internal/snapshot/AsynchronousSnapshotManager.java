package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.protocols.raft.internal.RaftEventLoop;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkRequest;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkResponse;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotMetadataRequest;
import org.jgroups.raft.AsyncSnapshot;
import org.jgroups.raft.SnapshotHandle;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Asynchronous snapshot manager that offloads state machine serialization to a background thread.
 *
 * <p>
 * Snapshot creation is split into two phases. The first phase runs on the RAFT event loop: it serializes the persistent
 * state and captures a frozen view of the state machine via {@link AsyncSnapshot#prepareSnapshot()}. The second phase runs
 * on a background thread obtained from {@link RaftEventLoop#executor()}: it serializes the captured state via
 * {@link SnapshotHandle#writeTo(java.io.DataOutput)} and submits the result back to the event loop for persistence. The
 * event loop is free to process commits and appends during the second phase.
 * </p>
 *
 * <p>
 * At most one snapshot creation can be in progress at any time. Concurrent calls to {@link #create(long, PostCreateAction)}
 * return immediately without effect while a snapshot is in flight.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotManager
 * @see AsyncSnapshot
 */
final class AsynchronousSnapshotManager implements SnapshotManager {

    private static final org.jgroups.logging.Log LOG = LogFactory.getLog(AsynchronousSnapshotManager.class);

    private static final int DEFAULT_BUFFER_SIZE = 1 << 20;
    private static final String PREFIX_SNAPSHOT_FILE = "snapshot-transfer.tmp";

    private final RaftEventLoop eventLoop;
    private final AsyncSnapshot asyncSnapshot;
    private final PersistentState persistentState;
    private final Log log;
    private final SnapshotSender sender;
    private final DefaultSnapshotMetrics metrics;

    private final Path baseLogDir;
    private final int chunkSize;
    private final int batchSize;

    private ActiveSnapshotTransfer transfer = null;

    // In progress is updated both from event loop and from background task.
    private volatile boolean inProgress;

    AsynchronousSnapshotManager(
            RaftEventLoop eventLoop,
            AsyncSnapshot asyncSnapshot,
            PersistentState persistentState,
            Log log,
            SnapshotSender sender,
            DefaultSnapshotMetrics metrics,
            Path baseLogDir,
            int chunkSize,
            int batchSize
    ) {
        this.eventLoop = eventLoop;
        this.asyncSnapshot = asyncSnapshot;
        this.persistentState = persistentState;
        this.log = log;
        this.sender = sender;
        this.metrics = metrics;
        this.baseLogDir = baseLogDir;
        this.chunkSize = chunkSize;
        this.batchSize = batchSize;
    }

    @Override
    public boolean create(long commitIndex, PostCreateAction action) throws Exception {
        if (inProgress)
            return false;

        inProgress = true;

        // We first copy over all the internal state necessary for RAFT itself.
        // We only offload the state machine to another thread.
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(DEFAULT_BUFFER_SIZE, true);
        persistentState.writeTo(out);

        SnapshotHandle handle;
        try {
            handle = asyncSnapshot.prepareSnapshot();
        } catch (Exception e) {
            inProgress = false;
            metrics.snapshotFailedCreate();
            throw e;
        }

        try {
            Executor executor = eventLoop.executor();
            executor.execute(new BackgroundSnapshotRunnable(commitIndex, out, action, handle));
            return true;
        } catch (Exception e) {
            LOG.error("Failed submitting asynchronous snapshot task", e);
            handle.release();
            inProgress = false;
            metrics.snapshotFailedCreate();
            throw e;
        }
    }

    @Override
    public void transferTo(Message message, RaftHeader hdr, long lastIndex, long lastTerm, Address dest) throws Exception {
        if (hdr == null) {
            long totalSize = log.snapshotSize();

            LOG.debug("Sending snapshot metadata to %s (index=%d, term=%d, size=%d)", dest, lastIndex, lastTerm, totalSize);
            sender.sendMetadata(dest, log.currentTerm(), lastIndex, lastTerm, totalSize);
            return;
        }

        if (!(hdr instanceof SnapshotChunkRequest scr)) {
            LOG.warn("Unexpected request for async snapshot manager: %s", hdr);
            return;
        }

        int startChunk = scr.startChunk();
        int count = scr.count();
        long lastIncludedIndex = scr.lastIncludedIndex();
        long currentTerm = log.currentTerm();
        long totalSize = log.snapshotSize();

        for (int i = 0; i < count; i++) {
            // Allocate a buffer per invocation.
            // This buffer will be submitted internally through JGroups.
            // We prefer to play safe and create a new copy instead of share/reuse the buffer.
            byte[] buf = new byte[chunkSize];
            int chunkIndex = startChunk + i;
            long offset = (long) chunkIndex * chunkSize;

            if (offset >= totalSize)
                break;

            int len = (int) Math.min(chunkSize, totalSize - offset);

            // To keep things simple, we read the snapshot from the event loop.
            // This alleviates us from having to implement some synchronization machinery for snapshot read/write.
            // For example, reading outside the event loop, the snapshot could be swapped while reading chunks.
            // This would allow to transfer corrupted chunks that could go unnoticed.
            int read = log.readSnapshotRegion(offset, buf, 0, len);
            if (read <= 0) {
                LOG.debug("Reading from snapshot returned earlier. Read=%d and expected=%d", read, len);
                break;
            }

            boolean done = offset + read >= totalSize;
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, read);

            // We offload the operation submit to outisde the event loop so we can continue processing.
            eventLoop.executor().execute(() -> sender.sendChunkResponse(dest, currentTerm, lastIncludedIndex, bb, offset, done));
        }
    }

    @Override
    public void install(ByteBuffer data, RaftHeader hdr, PostInstallAction action) throws Exception {
        LOG.debug("Restoring state machine with snapshot (%d bytes) - %s", data.remaining(), hdr);

        if (hdr instanceof SnapshotMetadataRequest smr) {
            handleMetadata(smr, action);
            return;
        }

        if (hdr instanceof SnapshotChunkResponse scr) {
            handleChunkResponse(data, scr);
            return;
        }

        LOG.warn("Unexpected header type in async install: %s", hdr);
    }

    private void handleMetadata(SnapshotMetadataRequest smr, PostInstallAction action) throws IOException {
        if (transfer != null) {
            if (transfer.lastIncludedIndex == smr.lastIncludedIndex() && Objects.equals(transfer.leader(), smr.leader())) {
                LOG.debug("Repeated request for same index, and transfer already in progress, returning: %s", smr);
                return;
            }

            abortActiveTransfer();
        }

        Path tempFile = temporaryFileLocation();
        FileChannel channel = FileChannel.open(tempFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING);

        ChunkTracker tracker = new ChunkTracker(smr.totalSize(), chunkSize, batchSize);
        transfer = new ActiveSnapshotTransfer(
                tracker, smr.lastIncludedIndex(), smr.lastIncludedTerm(), smr.currTerm(), smr.leader(), channel, action);

        metrics.chunkTransferStarted();

        LOG.debug("Starting chunk transfer from %s -> %s", smr, transfer);

        int initialBatch = Math.min(batchSize, tracker.totalChunks());
        sender.sendChunkRequest(smr.leader(), smr.currTerm(), smr.lastIncludedIndex(), 0, initialBatch);
        tracker.markRequested(initialBatch);
    }

    private void handleChunkResponse(ByteBuffer buffer, SnapshotChunkResponse scr) throws Exception {
        if (transfer == null) {
            LOG.warn("Received chunk response without active transfer: %s", scr);
            return;
        }

        if (scr.lastIncludedIndex() != transfer.lastIncludedIndex) {
            LOG.debug("Received chunk of a different snapshot recv=%s / transfer=%s", scr, transfer);
            return;
        }

        int chunkBytes = buffer.remaining();

        long writeOffset = scr.offset();
        while (buffer.hasRemaining()) {
            writeOffset += transfer.channel.write(buffer, writeOffset);
        }
        transfer.tracker.markReceived(scr.offset(), chunkBytes);

        metrics.chunkReceived(chunkBytes);
        metrics.updateTransferProgress(
                transfer.tracker.totalChunks(),
                transfer.tracker.received(),
                transfer.tracker.inFlight(),
                transfer.tracker.highestRequested(),
                transfer.tracker.missingChunks());

        if (transfer.tracker.isComplete()) {
            completeActiveTransfer();
            return;
        }

        if (transfer.tracker.shouldRefill()) {
            int count = transfer.tracker.refillCount();
            int start = transfer.tracker.nextRequestStart();
            sender.sendChunkRequest(transfer.leader, transfer.currentTerm, transfer.lastIncludedIndex, start, count);
            transfer.tracker.markRequested(count);
        }
    }

    private void abortActiveTransfer() {
        if (transfer == null)
            return;

        LOG.debug("Aborting chunked snapshot transfer %s", transfer);

        metrics.chunkTransferFailed();
        cleanupActiveTransfer();
    }

    private void cleanupActiveTransfer() {
        if (transfer == null)
            return;

        ActiveSnapshotTransfer ast = transfer;
        transfer = null;

        Util.close(ast.channel);
        try {
            Files.deleteIfExists(temporaryFileLocation());
        } catch (IOException e) {
            LOG.warn("Failed to delete temporary snapshot file: %s", e.getMessage());
        }

        metrics.clearTransferProgress();
    }

    private void completeActiveTransfer() throws Exception {
        try {
            LOG.debug("Completing active snapshot entry: %s", transfer);

            transfer.channel.position(0);
            log.setSnapshot(Channels.newInputStream(transfer.channel));

            transfer.channel.position(0);
            DataInput in = new DataInputStream(new BufferedInputStream(Channels.newInputStream(transfer.channel)));
            persistentState.readFrom(in);
            asyncSnapshot.readContentFrom(in);

            LogEntry le = new LogEntry(transfer.lastIncludedTerm, null);
            log.reinitializeTo(transfer.lastIncludedIndex, le);

            transfer.action.onSnapshotInstalled(transfer.lastIncludedIndex, transfer.lastIncludedTerm);

            metrics.chunkTransferCompleted();
            metrics.snapshotReceived();

            LOG.debug("Installed chunked snapshot: %s", transfer);
        } catch (Exception e) {
            metrics.chunkTransferFailed();
            metrics.snapshotFailedInstall();
            throw e;
        } finally {
            cleanupActiveTransfer();
        }
    }

    private Path temporaryFileLocation() {
        return baseLogDir.resolve(PREFIX_SNAPSHOT_FILE);
    }

    @Override
    public SnapshotMetrics metrics() {
        return metrics;
    }

    private final class BackgroundSnapshotRunnable implements Runnable {

        private final ByteArrayDataOutputStream out;
        private final SnapshotHandle handle;
        private final PostCreateAction action;
        private final long commitIndex;

        private BackgroundSnapshotRunnable(long commitIndex, ByteArrayDataOutputStream out, PostCreateAction action, SnapshotHandle handle) {
            this.commitIndex = commitIndex;
            this.out = out;
            this.action = action;
            this.handle = handle;
        }

        @Override
        public void run() {
            try {
                LOG.debug("Starting asynchronous snapshot on state machine");
                handle.writeTo(out);
                ByteBuffer buffer = ByteBuffer.wrap(out.buffer(), 0, out.position());
                CompletionStage<Void> cs = eventLoop.submit(() -> {
                    log.setSnapshot(buffer);
                    action.onSnapshotDone(commitIndex);
                    return null;
                });
                cs.whenComplete((ignore, t) -> {
                    if (t != null) {
                        metrics.snapshotFailedCreate();
                    } else {
                        metrics.snapshotCreated();
                    }

                    if (LOG.isDebugEnabled())
                        LOG.debug("Finished taking async snapshot (%s bytes) index=%d", Util.printBytes(out.position()), commitIndex);
                    handle.release();
                    inProgress = false;
                });
            } catch (Exception e) {
                LOG.error("Failed creating asynchronous snapshot", e);
                metrics.snapshotFailedCreate();
                handle.release();
                inProgress = false;
            }
        }
    }

    private record ActiveSnapshotTransfer(
            ChunkTracker tracker, long lastIncludedIndex, long lastIncludedTerm,
            long currentTerm, Address leader, FileChannel channel, PostInstallAction action) { }
}
