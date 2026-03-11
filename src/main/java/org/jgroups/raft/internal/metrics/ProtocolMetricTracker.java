package org.jgroups.raft.internal.metrics;

import org.jgroups.raft.metrics.LatencyMetrics;

/**
 * Base class for per-protocol metric trackers.
 *
 * <p>
 * Each Raft protocol (RAFT, REDIRECT, Election) owns its own metric tracker that manages latency recording independently.
 * This ensures protocol-level isolation — no shared mutable state between protocols.
 * </p>
 *
 * <p>
 * Subclasses choose between concurrent and single-writer trackers based on their threading model:
 * <ul>
 *   <li>Concurrent: for protocols with multi-threaded completion paths (RAFT, REDIRECT)</li>
 *   <li>Single-writer: for protocols where all writes come from one thread (Election)</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public sealed abstract class ProtocolMetricTracker permits ElectionProtocolMetrics, RaftProtocolMetrics, RedirectProtocolMetrics {

    /**
     * Creates a thread-safe latency tracker backed by {@code ConcurrentHistogram}.
     *
     * <p>
     * Use when latency recording can happen from multiple threads, such as
     * the RAFT event loop and network/election threads completing requests.
     * </p>
     *
     * @return a new concurrent latency tracker.
     */
    static LatencyTracker createConcurrentTracker() {
        return new LatencyTracker(true);
    }

    /**
     * Creates a non-thread-safe latency tracker backed by a plain {@code Histogram}.
     *
     * <p>
     * Use only when all writes are guaranteed to come from a single thread.
     * Avoids CAS overhead on every {@code recordValue()} call.
     * </p>
     *
     * @return a new single-writer latency tracker.
     */
    static LatencyTracker createSingleWriterTracker() {
        return new LatencyTracker(false);
    }

    /**
     * Returns a disabled {@link LatencyMetrics} instance that returns {@code -1}
     * for all metrics. Used when stats collection is disabled.
     *
     * @return a disabled latency metrics instance.
     */
    static LatencyMetrics disabledLatencyMetrics() {
        return LatencyMetrics.disabled();
    }

}
