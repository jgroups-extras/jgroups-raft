package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.raft.StateMachine;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.nio.ByteBuffer;

/**
 * Synchronous snapshot manager that blocks the calling thread for the entire duration of each operation.
 *
 * <p>
 * Snapshot creation serializes the persistent state and state machine inline, so the RAFT event loop is stalled until the
 * write completes. Installation and transfer follow the same blocking model. Suitable for state machines that do not implement
 * {@link org.jgroups.raft.AsyncSnapshot}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotManager
 */
final class SynchronousSnapshotManager implements SnapshotManager {
    private static final org.jgroups.logging.Log LOG = LogFactory.getLog(SynchronousSnapshotManager.class);

    private static final int DEFAULT_BUFFER_SIZE = 1 << 20;

    private final StateMachine stateMachine;
    private final PersistentState persistentState;
    private final Log log;
    private final SnapshotSender sender;
    private final DefaultSnapshotMetrics metrics;

    SynchronousSnapshotManager(
            StateMachine stateMachine,
            PersistentState persistentState,
            Log log,
            SnapshotSender sender,
            DefaultSnapshotMetrics metrics
    ) {
        this.stateMachine = stateMachine;
        this.persistentState = persistentState;
        this.log = log;
        this.sender = sender;
        this.metrics = metrics;
    }

    @Override
    public void create(long commitIndex, PostCreateAction action) throws Exception {
        if (stateMachine == null)
            throw new IllegalStateException("State machine is not defined");

        try {
            LOG.debug("Taking snapshot");
            ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(DEFAULT_BUFFER_SIZE, true);
            persistentState.writeTo(out);
            stateMachine.writeContentTo(out);

            ByteBuffer buffer = ByteBuffer.wrap(out.buffer(), 0, out.position());
            log.setSnapshot(buffer);
            action.onSnapshotDone(commitIndex);
            metrics.snapshotCreated();
        } catch (Exception e) {
            metrics.snapshotFailedCreate();
            throw e;
        }
    }

    @Override
    public void transferTo(Address dest, long lastIndex, long lastTerm) throws Exception {
        ByteBuffer data = log.getSnapshot();

        LOG.debug("Sending snapshot (%s), to %s (%d - %d)", Util.printBytes(data.position()), lastIndex, lastTerm);
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
            stateMachine.readContentFrom(in);

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
}
