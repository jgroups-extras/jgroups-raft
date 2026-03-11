package org.jgroups.raft.internal.metrics;

import org.jgroups.raft.metrics.LatencyMetrics;

/**
 * Metric tracker for the {@link org.jgroups.protocols.raft.RAFT} protocol.
 *
 * <p>
 * Tracks two latency dimensions for write and read-only operations:
 * <ul>
 *   <li><b>Total latency:</b> end-to-end from request submission to completion, including queue wait, log append, and replication.</li>
 *   <li><b>Processing latency:</b> from event loop pickup to completion, excluding queue wait time.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Both trackers use {@code ConcurrentHistogram} because request completion can happen from multiple threads
 * (event loop, network, election).
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public final class RaftProtocolMetrics extends ProtocolMetricTracker {
    private final LatencyTracker totalTracker;
    private final LatencyTracker processingTracker;

    public RaftProtocolMetrics() {
        this.totalTracker = createConcurrentTracker();
        this.processingTracker = createConcurrentTracker();
    }

    public void recordTotalLatency(long latencyNanos) {
        totalTracker.recordLatency(latencyNanos);
    }

    public void recordProcessingLatency(long latencyNanos) {
        processingTracker.recordLatency(latencyNanos);
    }

    public LatencyMetrics total() {
        return totalTracker;
    }

    public LatencyMetrics processing() {
        return processingTracker;
    }
}
