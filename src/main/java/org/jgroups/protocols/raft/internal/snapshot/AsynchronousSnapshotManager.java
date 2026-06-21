package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.protocols.raft.internal.RaftEventLoop;
import org.jgroups.raft.AsyncSnapshot;
import org.jgroups.raft.SnapshotHandle;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.nio.ByteBuffer;
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

    private final RaftEventLoop eventLoop;
    private final AsyncSnapshot asyncSnapshot;
    private final PersistentState persistentState;
    private final Log log;
    private final SnapshotSender sender;
    private final DefaultSnapshotMetrics metrics;

    private volatile boolean inProgress;

    AsynchronousSnapshotManager(
            RaftEventLoop eventLoop,
            AsyncSnapshot asyncSnapshot,
            PersistentState persistentState,
            Log log,
            SnapshotSender sender,
            DefaultSnapshotMetrics metrics
    ) {
        this.eventLoop = eventLoop;
        this.asyncSnapshot = asyncSnapshot;
        this.persistentState = persistentState;
        this.log = log;
        this.sender = sender;
        this.metrics = metrics;
    }

    @Override
    public void create(long commitIndex, PostCreateAction action) throws Exception {
        if (inProgress)
            return;

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
        } catch (Exception e) {
            LOG.error("Failed submitting asynchronous snapshot task", e);
            handle.release();
            inProgress = false;
            metrics.snapshotFailedCreate();
            throw e;
        }
    }

    @Override
    public void transferTo(Address dest, long lastIndex, long lastTerm) throws Exception {
        ByteBuffer data = log.getSnapshot();

        if (LOG.isDebugEnabled())
            LOG.debug("Sending snapshot (%s), to %s (index=%d, term=%d)", Util.printBytes(data.position()), dest, lastIndex, lastTerm);

        sender.send(dest, data, lastIndex, lastTerm);
    }

    @Override
    public void install(ByteBuffer data, long lastIncludedIndex, long lastIncludedTerm, PostInstallAction action) throws Exception {
        LOG.debug("Restoring state machine with snapshot (%d bytes), (index=%d, term=%d)", data.remaining(), lastIncludedIndex, lastIncludedTerm);

        int pos = data.position();
        log.setSnapshot(data);
        data.position(pos);

        try {
            DataInput in = new ByteArrayDataInputStream(data);
            persistentState.readFrom(in);
            asyncSnapshot.readContentFrom(in);

            LogEntry le = new LogEntry(lastIncludedTerm, null);
            log.reinitializeTo(lastIncludedIndex, le);
            action.onSnapshotInstalled(lastIncludedIndex, lastIncludedTerm);
            metrics.snapshotReceived();
        } catch (Exception e) {
            metrics.snapshotFailedInstall();
            throw e;
        }
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
}
