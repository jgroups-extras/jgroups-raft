package org.jgroups.protocols.raft;

/**
 * Thrown when a request is submitted to a node that is not the current Raft leader.
 *
 * <p>
 * Only the elected leader can accept write requests. When a non-leader node receives a write, it rejects it with this
 * exception. Clients should retry against the current leader, whose
 * address is available via {@link RAFT#leader()}.
 * </p>
 *
 * @since 2.0
 */

public final class RaftLeaderException extends RuntimeException {
    public RaftLeaderException(String s) {
        super(s);
    }
}
