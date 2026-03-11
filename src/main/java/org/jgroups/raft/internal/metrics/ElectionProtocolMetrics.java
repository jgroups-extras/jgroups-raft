package org.jgroups.raft.internal.metrics;

import org.jgroups.raft.metrics.LatencyMetrics;

import net.jcip.annotations.NotThreadSafe;

/**
 * Metric tracker for the election protocol ({@link org.jgroups.protocols.raft.election.BaseElection}).
 *
 * <p>
 * Tracks the election latency: the time from when the voting thread starts until a leader is elected and announced to
 * the cluster.
 * </p>
 *
 * <p>
 * Uses a plain {@code Histogram} because all recordings happen from the single voting thread.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@NotThreadSafe
public final class ElectionProtocolMetrics extends ProtocolMetricTracker {

    private final LatencyTracker electionTracker;

    public ElectionProtocolMetrics() {
        this.electionTracker = createSingleWriterTracker();
    }

    /**
     * Records the elapsed time of a completed leader election.
     *
     * @param latencyNanos elapsed time in nanoseconds.
     */
    public void recordElectionLatency(long latencyNanos) {
        electionTracker.recordLatency(latencyNanos);
    }

    /**
     * @return the election latency metrics.
     */
    public LatencyMetrics election() {
        return electionTracker;
    }
}
