package org.jgroups.raft.metrics;

/**
 * Performance of each sub-system of the algorithm.
 *
 * // Explain on each method how the metrics are measured.
 *
 * @since 2.0
 * @author José Bolina
 */
public interface PerformanceMetrics {

    LatencyMetrics getCommandProcessingLatency();

    LatencyMetrics getLeaderElectionLatency();

    LatencyMetrics getReplicationLatency();
}
