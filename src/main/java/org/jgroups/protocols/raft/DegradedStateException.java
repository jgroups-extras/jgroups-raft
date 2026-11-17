package org.jgroups.protocols.raft;

/**
 * Thrown when a node in degraded state rejects a new request.
 *
 * <p>
 * A node enters degraded state when a critical local failure occurs, such as a state machine that throws while applying
 * a committed command, or a storage failure in the Raft log. Once degraded, the node stops applying commands and cannot
 * safely process new requests, because its local state may have diverged from the cluster. The node must be restarted to
 * recover, after fixing the cause of the failure. On restart, it rebuilds its state from the latest snapshot and the log.
 * </p>
 *
 * @since 2.0
 */
public final class DegradedStateException extends RuntimeException {
    public DegradedStateException(String message) {
        super(message);
    }
}
