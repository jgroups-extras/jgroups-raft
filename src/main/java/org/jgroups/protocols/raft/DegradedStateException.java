package org.jgroups.protocols.raft;

/**
 * Thrown when a node in degraded state rejects a new request.
 *
 * <p>
 * A node enters degraded state when a critical failure occurs during operation, such as a
 * {@link org.jgroups.raft.StateMachine#apply state machine apply} failure or a storage failure in the Raft log. Once degraded,
 * the node cannot safely process new requests because its local state may have diverged from the cluster. The node must
 * be restarted to recover, and follow the necessary procedures to fix any failures reported. On restart, it replays committed
 * entries from the Raft log (or loads from a snapshot) to rebuild consistent state.
 * </p>
 *
 * @since 2.0
 */
public final class DegradedStateException extends RuntimeException {
    public DegradedStateException(String message) {
        super(message);
    }
}
