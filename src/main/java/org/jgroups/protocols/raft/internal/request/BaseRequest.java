package org.jgroups.protocols.raft.internal.request;

/**
 * A unit of work submitted to the RAFT event loop for single-threaded processing.
 *
 * <p>
 * Defines lifecycle hooks for latency tracking at two granularities:
 * <ul>
 *   <li><b>Total</b> — end-to-end from submission to completion.</li>
 *   <li><b>Processing</b> — from event loop processing to commit (majority reached).</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public sealed interface BaseRequest permits DownRequest, UntrackedRequest {

    /**
     * Marks the start of the total span, before the request enters the event loop queue.
     */
    void startTotal();

    /**
     * Marks the end of the total span, after the request completes or fails.
     */
    void completeTotal();

    /**
     * Marks the start of the processing span, when the event loop begins processing.
     */
    void startProcessing();

    /**
     * Marks the end of the processing span, when a majority commits the entry.
     */
    void completeProcessing();

    /**
     * Signals that the request has failed.
     *
     * @return {@code true} if this call caused the transition to the failed state.
     */
    default boolean failed(Throwable t) {
        return false;
    }
}
