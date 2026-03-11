package org.jgroups.raft.internal.metrics;

import org.jgroups.raft.metrics.LatencyMetrics;

/**
 * Metric tracker for the {@link org.jgroups.protocols.raft.REDIRECT} protocol.
 *
 * <p>
 * Tracks the redirect round-trip latency: the time from when a follower forwards a request
 * to the leader until it receives the response back.
 * </p>
 *
 * <p>
 * Uses {@code ConcurrentHistogram} because responses arrive on transport threads.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public final class RedirectProtocolMetrics extends ProtocolMetricTracker {
    private final LatencyTracker redirectTracker;

    public RedirectProtocolMetrics() {
        this.redirectTracker = createConcurrentTracker();
    }

    public void recordRedirectLatency(long latencyNanos) {
        redirectTracker.recordLatency(latencyNanos);
    }

    public LatencyMetrics redirect() {
        return redirectTracker;
    }
}
