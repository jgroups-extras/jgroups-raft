package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.protocols.raft.internal.RaftEventLoop;
import org.jgroups.raft.AsyncSnapshot;

import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * Manages snapshot creation and transfer for a RAFT node.
 *
 * <p>
 * The concrete implementation determines whether snapshots are created synchronously or asynchronously based on the state
 * machine's capabilities. Callers interact only with this interface and are unaware of the strategy.
 * </p>
 *
 * <p>
 * Obtain an instance via the {@link #create(long, PostCreateAction)} factory method, which inspects the registered state machine and returns
 * the appropriate implementation.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotMetrics
 */
public sealed interface SnapshotManager permits AsynchronousSnapshotManager, SynchronousSnapshotManager {

    /**
     * Callback invoked when a snapshot has been fully serialized and is ready to be persisted.
     *
     * <p>
     * The handler always executes on the RAFT event loop. For synchronous snapshots it runs inline within {@link #create}.
     * For asynchronous snapshots it runs via a {@code CallableDownRequest} submitted to the event loop after background
     * serialization completes.
     * </p>
     */
    @FunctionalInterface
    interface PostCreateAction {

        /**
         * Receives the serialized snapshot and the commit index captured at creation time.
         *
         * @param capturedIndex the commit index at the time the snapshot was initiated,
         *                      suitable for log truncation
         * @throws Exception if persisting the snapshot or truncating the log fails
         */
        void onSnapshotDone(long capturedIndex) throws Exception;
    }

    /**
     * Callback invoked after a snapshot from a leader has been fully installed.
     *
     * <p>
     * The snapshot data has already been persisted, the state machine restored, and the log reinitialized when this callback
     * fires. The implementation is responsible for updating protocol-level state (e.g., commit index, last appended) and
     * acknowledging the leader.
     * </p>
     */
    @FunctionalInterface
    interface PostInstallAction {

        /**
         * Called after successful snapshot installation.
         *
         * @param lastIncludedIndex the last log index reflected in the installed snapshot
         * @param lastIncludedTerm the term of the last log entry reflected in the installed snapshot
         * @throws Exception if the post-install protocol actions fail
         */
        void onSnapshotInstalled(long lastIncludedIndex, long lastIncludedTerm) throws Exception;
    }

    /**
     * Creates a snapshot of the current state machine.
     *
     * <p>
     * Serializes the cluster internal state and the state machine contents, then delivers the result to the provided
     * post-complete action. The action is responsible for persisting the snapshot to the log and performing any post-snapshot
     * cleanup (e.g., log truncation, size counter reset).
     * </p>
     *
     * <p>
     * If an asynchronous snapshot is already in progress, this method returns immediately without invoking the handler.
     * </p>
     *
     * @param commitIndex the commit index at the time the snapshot is requested
     * @param action      receives the serialized snapshot on the event loop
     * @return <code>true</code> if taking a snapshot. <code>false</code>, otherwise.
     * @throws Exception if snapshot creation fails synchronously
     */
    boolean create(long commitIndex, PostCreateAction action) throws Exception;

    /**
     * Sends a snapshot to a lagging follower.
     *
     * @param message if there is an underlying message triggering the transfer. <code>null</code>, otherwise.
     * @param hdr the header associated with the provided message. Can be <code>null</code>
     * @param lastIndex the last committed log index reflected in the snapshot
     * @param lastTerm  the term of the last committed log entry
     * @param dest      the follower's address
     * @throws Exception if the snapshot cannot be read or the message cannot be sent
     */
    void transferTo(Message message, RaftHeader hdr, long lastIndex, long lastTerm, Address dest) throws Exception;

    /**
     * Installs a snapshot received from the leader.
     *
     * <p>
     * Persists the snapshot, restores the cluster internal state and state machine from the received data, and reinitializes
     * the log at {@code lastIncludedIndex}. On success, invokes the provided action so the caller can update protocol-level
     * state and acknowledge the leader.
     * </p>
     *
     * <p>
     * If installation fails, the action is not invoked and the exception propagates to the caller.
     * </p>
     *
     * @param data the raw snapshot bytes
     * @param hdr the request header with additional information
     * @param action callback for post-install protocol actions
     * @throws Exception if the snapshot cannot be installed
     */
    void install(ByteBuffer data, RaftHeader hdr, PostInstallAction action) throws Exception;

    /**
     * Returns the metrics view for snapshot operations.
     *
     * @return the snapshot metrics, never {@code null}
     */
    SnapshotMetrics metrics();

    /**
     * Creates a {@code SnapshotManager} appropriate for the registered state machine.
     *
     * <p>
     * If the state machine implements {@link org.jgroups.raft.AsyncSnapshot}, returns an asynchronous manager that performs
     * two-phase snapshot creation. Otherwise, returns a synchronous manager that blocks the event loop during serialization.
     * The consumer of manager should not care about a synchronous or asynchronous snapshot is taking place.
     * </p>
     *
     * @param raft the RAFT protocol instance
     * @param persistentState raft internal state, like membership changes, need to be serialized, too
     * @param eventLoop internal RAFT event loop to submit operations
     * @return a new snapshot manager
     */
    static SnapshotManager create(RAFT raft, PersistentState persistentState, RaftEventLoop eventLoop) {
        DefaultSnapshotMetrics metrics = new DefaultSnapshotMetrics(raft.timeService());
        SnapshotSender sender = new JGroupsRaftSnapshotSender(raft);
        if (raft.stateMachine() instanceof AsyncSnapshot as) {
            return new AsynchronousSnapshotManager(eventLoop, as, persistentState, raft.log(), sender, metrics,
                    Path.of(raft.logDir()), raft.snapshotChunkSize(), raft.snapshotBatchSize());
        }

        return new SynchronousSnapshotManager(
                raft.stateMachine(),
                persistentState,
                raft.log(),
                sender,
                metrics
        );
    }
}
