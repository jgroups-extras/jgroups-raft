package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.protocols.raft.StagedSnapshotCapability;
import org.jgroups.protocols.raft.internal.RaftEventLoop;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkRequest;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkResponse;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotMetadataRequest;
import org.jgroups.raft.AsyncSnapshot;
import org.jgroups.raft.SnapshotHandle;
import org.jgroups.util.Util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
    private static final String PREFIX_SNAPSHOT_WRITE_FILE = "snapshot-write.tmp";

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
        OutputStream out;
        FileChannel channel;

        StagedSnapshotCapability capability = log.findCapability(StagedSnapshotCapability.class);
        if (capability != null) {
            // Since the log supports a two-phase snapshot writing, we utilize the provided arena to write the snapshot.
            // This way, we don't need to utilize a explicit file channel.
            out = capability.stage();
            channel = null;
        } else {
            Files.createDirectories(baseLogDir);
            Path tempFile = temporaryWriteFileLocation();
            channel = FileChannel.open(tempFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING);

            // We first copy over all the internal state necessary for RAFT itself.
            // We only offload the state machine to another thread.
            out = Channels.newOutputStream(channel);
        }

        // Ensure the internal Raft state is serialized in the current event loop thread before delegating.
        DataOutputStream dos = new DataOutputStream(out);
        persistentState.writeTo(dos);
        dos.flush();

        SnapshotHandle handle;
        try {
            handle = asyncSnapshot.prepareSnapshot();
        } catch (Exception e) {
            inProgress = false;
            metrics.snapshotFailedCreate();
            deleteSnapshotWriteFile(channel, dos);
            throw e;
        }

        try {
            // We dispatch only the state machine serialization to a background thread.
            // The provided action run at the end is guaranteed to run in the event loop.
            Executor executor = eventLoop.executor();
            executor.execute(new BackgroundSnapshotRunnable(commitIndex, channel, out, action, handle));
            return true;
        } catch (Exception e) {
            LOG.error("Failed submitting asynchronous snapshot task", e);
            handle.release();
            inProgress = false;
            metrics.snapshotFailedCreate();
            deleteSnapshotWriteFile(channel, dos);
            throw e;
        }
    }

    @Override
    public void transferTo(Message message, RaftHeader hdr, long lastIndex, long lastTerm, Address dest) throws Exception {
        // The transfer mechanism is executed only at the Raft leader.
        // The followers are the ones requesting the snapshot from the leader when they lag behind.
        // If this is the first request, the leader only submits to the follower information about the snapshot.
        // The follower is then responsible to continue requesting chunks of the snapshot, the leader doesn't hold any state for that.
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

        // The follower is the one for keeping state about the snapshot transfer state.
        // The follower will request the specific chunks it needs to create the complete snapshot.
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
        // Snapshot installation is only called at nodes that are lagging behind.
        // These nodes will be responsible to contact the leader and request chunks.
        LOG.debug("Restoring state machine with snapshot (%d bytes) - %s", data.remaining(), hdr);

        // Metadata contains only information about the snapshot.
        // The follower utilizes the metdata to calculate the chunks and manage the requests.
        // The metadata is the very first message sent from the leader to the follower before starting the transfer.
        if (hdr instanceof SnapshotMetadataRequest smr) {
            handleMetadata(smr, action);
            return;
        }

        // The follower receives a chunk with data.
        // This message will contain a piece of the full snapshot data.
        // The follower is then responsible to flush this information to disk and continue requesting chunks.
        if (hdr instanceof SnapshotChunkResponse scr) {
            handleChunkResponse(data, scr);
            return;
        }

        LOG.warn("Unexpected header type in async install: %s", hdr);
    }

    private void handleMetadata(SnapshotMetadataRequest smr, PostInstallAction action) throws IOException {
        if (transfer != null) {
            // Metadata is only ever received to START a transfer.
            // A follower would only ever have a single transfer in progress at any given time.
            // If there is a transfer in progress, we double-check whether it is the same transfer.
            if (transfer.lastIncludedIndex == smr.lastIncludedIndex() && Objects.equals(transfer.leader(), smr.leader())) {
                LOG.debug("Repeated request for same index, and transfer already in progress, returning: %s", smr);
                return;
            }

            // If this is a request from another transfer, e.g., there was a leader change, we abort and start a new one.
            abortActiveTransfer();
        }

        // Initialize the files to store the chunks while receiving them.
        Files.createDirectories(baseLogDir);
        Path tempFile = temporaryFileLocation();
        FileChannel channel = FileChannel.open(tempFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING);

        // Initialize the tracker to identify the missing chunks and calculate metrics.
        ChunkTracker tracker = new ChunkTracker(smr.totalSize(), chunkSize, batchSize);
        transfer = new ActiveSnapshotTransfer(
                tracker, smr.lastIncludedIndex(), smr.lastIncludedTerm(), smr.currTerm(), smr.leader(), channel, action);

        metrics.chunkTransferStarted();

        LOG.debug("Starting chunk transfer from %s -> %s", smr, transfer);

        // Start requesting the chunks from the leader.
        // The follower will request N chunks at a time to try and keep the pipeline busy with several smaller messages.
        int initialBatch = Math.min(batchSize, tracker.totalChunks());
        sender.sendChunkRequest(smr.leader(), smr.currTerm(), smr.lastIncludedIndex(), 0, initialBatch);
        tracker.markRequested(initialBatch);
    }

    private void handleChunkResponse(ByteBuffer buffer, SnapshotChunkResponse scr) throws Exception {
        if (transfer == null) {
            LOG.warn("Received chunk response without active transfer: %s", scr);
            return;
        }

        // Double-check that the chunk belongs to the current active transfer.
        // The state machine progresses deterministically.
        // A snapshot taken at an index N will always match, regardless of which node it was taken.
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

            // Copies the temporary file holding the snapshot chunks into its final location.
            // The current transfer.channel holds only the raw data of the snapshot.
            // After setting the snapshot, it will follow the internal format of the Log implementation.
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

    private Path temporaryWriteFileLocation() {
        return baseLogDir.resolve(PREFIX_SNAPSHOT_WRITE_FILE);
    }

    @Override
    public SnapshotMetrics metrics() {
        return metrics;
    }

    private final class BackgroundSnapshotRunnable implements Runnable {

        private final FileChannel channel;
        private final DataOutputStream out;
        private final OutputStream originalOut;
        private final SnapshotHandle handle;
        private final PostCreateAction action;
        private final long commitIndex;

        private BackgroundSnapshotRunnable(long commitIndex, FileChannel channel, OutputStream out, PostCreateAction action, SnapshotHandle handle) {
            this.commitIndex = commitIndex;
            this.channel = channel;
            this.out = new DataOutputStream(new BufferedOutputStream(out, DEFAULT_BUFFER_SIZE));
            this.originalOut = out;
            this.action = action;
            this.handle = handle;
        }

        @Override
        public void run() {
            try {
                LOG.debug("Starting asynchronous snapshot on state machine");
                handle.writeTo(out);
                out.flush();

                CompletionStage<Void> cs;

                // After everything is written, we update the snapshot in the log.
                // If the log supports a 2-phase snapshot write, we utilize it.
                // This allows us to flush the snapshot into disk _outside_ of the event loop.
                // And submit to the event loop only the file flip, which still ensures thread safety.
                StagedSnapshotCapability capability = log.findCapability(StagedSnapshotCapability.class);
                if (capability != null) {
                    // Close only when the capability is available.
                    // Otherwise, close will also close the underlying file channel.
                    out.close();
                    // Still on the background thread, write the data into the file.
                    // Flip to the new file in the event loop, since it writes through the Log interface.
                    cs = eventLoop.submit(() -> {
                        capability.commit(originalOut);
                        action.onSnapshotDone(commitIndex);
                        return null;
                    });
                } else {
                    // If the log implementation does support this 2-phase writes, we do everything in the event loop.
                    // This inevitably stalls the event loop until all the write and swap finishes.
                    cs = eventLoop.submit(() -> {
                        log.setSnapshot(Channels.newInputStream(channel.position(0)));
                        action.onSnapshotDone(commitIndex);
                        return null;
                    });
                }
                cs.whenComplete((ignore, t) -> {
                    if (t != null) {
                        metrics.snapshotFailedCreate();
                    } else {
                        metrics.snapshotCreated();
                    }

                    if (LOG.isDebugEnabled())
                        LOG.debug("Finished taking async snapshot index=%d", commitIndex);
                    handle.release();
                    inProgress = false;
                    deleteSnapshotWriteFile(channel, out);
                });
            } catch (Exception e) {
                LOG.error("Failed creating asynchronous snapshot", e);
                metrics.snapshotFailedCreate();
                handle.release();
                inProgress = false;
                deleteSnapshotWriteFile(channel, out);
            }
        }
    }

    private void deleteSnapshotWriteFile(FileChannel channel, DataOutputStream dos) {
        try {
            if (channel != null) {
                channel.close();
                Files.deleteIfExists(temporaryWriteFileLocation());
            }
            if (dos != null) {
                dos.close();
            }
        } catch (IOException e) {
            LOG.error("Failed closing snapshot file at %s", temporaryWriteFileLocation(), e);
        }
    }

    private record ActiveSnapshotTransfer(
            ChunkTracker tracker, long lastIncludedIndex, long lastIncludedTerm,
            long currentTerm, Address leader, FileChannel channel, PostInstallAction action) { }
}
