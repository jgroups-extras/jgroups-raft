package org.jgroups.raft.metrics;

/**
 * Latency distribution for a specific operation category.
 *
 * <p>
 * Provides statistical summaries (average, percentiles, and maximum) over all recorded latency samples for a given
 * operation type. Each method in {@link PerformanceMetrics} returns a {@link LatencyMetrics} instance scoped to that
 * category (e.g., total request latency, consensus latency, election latency, or redirect latency).
 * </p>
 *
 * <h2>Values and Units</h2>
 *
 * <p>
 * All latency values are reported in <b>nanoseconds</b>. Percentile values (p95, p99) represent the latency at or below
 * which that percentage of all recorded operations completed. For example, a p99 of 5,000,000 ns means 99% of operations
 * completed within 5 ms.
 * </p>
 *
 * <h2>Interpreting the Distribution</h2>
 *
 * <p>
 * The average alone can be misleading; a few slow operations can skew it significantly. Prefer percentile-based metrics
 * for SLA monitoring: p95 captures the typical worst case, while p99 reveals tail latency outliers. A large gap between
 * p95 and p99 suggests occasional spikes worth investigating. Use {@link #getPercentile(double)} for custom thresholds
 * beyond the predefined ones.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see PerformanceMetrics
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

    /**
     * The arithmetic mean of all recorded latency samples, in nanoseconds.
     *
     * <p>
     * Useful as a general health indicator, but can be skewed by outliers. Compare against percentile values for a more
     * accurate picture of typical performance.
     * </p>
     *
     * @return the average latency in nanoseconds, or {@code -1} if metrics are disabled.
     */
    double getAvgLatency();

    /**
     * The 99th percentile latency, in nanoseconds.
     *
     * <p>
     * 99% of all recorded operations completed at or below this latency. This metric captures tail latency and is the
     * most sensitive to performance regressions or transient issues like garbage collection pauses or network retransmissions.
     * </p>
     *
     * @return the p99 latency in nanoseconds, or {@code -1} if metrics are disabled.
     */
    double getP99Latency();

    /**
     * The 95th percentile latency, in nanoseconds.
     *
     * <p>
     * 95% of all recorded operations completed at or below this latency. This is a common choice for SLA targets as it
     * filters out rare outliers while still reflecting the experience of most requests.
     * </p>
     *
     * @return the p95 latency in nanoseconds, or {@code -1} if metrics are disabled.
     */
    double getP95Latency();

    /**
     * The highest latency recorded across all samples, in nanoseconds.
     *
     * <p>
     * Represents the absolute worst-case latency observed. Useful for capacity planning and for identifying the upper
     * bound of operation duration under the current workload.
     * </p>
     *
     * @return the maximum latency in nanoseconds, or {@code -1} if metrics are disabled.
     */
    double getMaxLatency();

    /**
     * The latency at a custom percentile, in nanoseconds.
     *
     * <p>
     * Use this when the predefined p95 and p99 do not match your SLA thresholds. For example, {@code getPercentile(99.9)}
     * returns the latency below which 99.9% of operations completed.
     * </p>
     *
     * @param p the percentile to query, between {@code 0} and {@code 100}.
     * @return the latency at the given percentile in nanoseconds, or {@code -1} if metrics are disabled.
     */
    double getPercentile(double p);

    /**
     * The total number of latency samples recorded.
     *
     * <p>
     * Use this to assess whether the statistical summaries are meaningful; a small sample count means percentile values
     * may not yet be representative. This value also serves as an operation counter for the category this instance tracks.
     * </p>
     *
     * @return the number of recorded measurements, or {@code -1} if metrics are disabled.
     */
    long getTotalMeasurements();
}
