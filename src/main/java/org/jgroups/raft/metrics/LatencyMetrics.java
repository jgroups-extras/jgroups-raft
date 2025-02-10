package org.jgroups.raft.metrics;

/**
 * Tracks overall replication latency.
 *
 * // Explain how metrics are measured.
 *
 * @since 2.0
 * @author José Bolina
 */
public interface LatencyMetrics {

    double getAvgLatency();

    double getP99Latency();

    double getP95Latency();

    double getMaxLatency();

    double getPercentile(double p);

    long getTotalMeasurements();
}
