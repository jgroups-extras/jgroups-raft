package org.jgroups.protocols.raft.internal.request;

/**
 * A unit of work submitted to the RAFT event loop for single-threaded processing.
 *
 * <p>
 * Defines lifecycle hooks for latency tracking at two granularities:
 * <ul>
 *   <li><b>User operation</b> — end-to-end from submission to completion (includes queue wait).</li>
 *   <li><b>Replication</b> — from event loop processing to commit (majority reached).</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public sealed interface BaseRequest permits DownRequest, UntrackedRequest {

    /**
     * Marks the start of the user operation span, before the request enters the event loop queue.
     */
    void startUserOperation();

    /**
     * Marks the end of the user operation span, after the request completes or fails.
     */
    void completeUserOperation();

    /**
     * Marks the start of the replication span, when the event loop begins processing.
     */
    void startReplication();

    /**
     * Marks the end of the replication span, when a majority commits the entry.
     */
    void completeReplication();

    /**
     * Signals that the request has failed.
     *
     * @return {@code true} if this call caused the transition to the failed state.
     */
    default boolean failed(Throwable t) {
        return false;
    }
}
