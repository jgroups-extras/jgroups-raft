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

    static LatencyMetrics disabled() {
        return new LatencyMetrics() {
            @Override
            public double getAvgLatency() {
                return -1;
            }

            @Override
            public double getP99Latency() {
                return -1;
            }

            @Override
            public double getP95Latency() {
                return -1;
            }

            @Override
            public double getMaxLatency() {
                return -1;
            }

            @Override
            public double getPercentile(double p) {
                return -1;
            }

            @Override
            public long getTotalMeasurements() {
                return -1;
            }
        };
    }

    double getAvgLatency();

    double getP99Latency();

    double getP95Latency();

    double getMaxLatency();

    double getPercentile(double p);

    long getTotalMeasurements();
}
