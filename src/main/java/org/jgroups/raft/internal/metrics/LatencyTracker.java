package org.jgroups.raft.internal.metrics;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.raft.metrics.LatencyMetrics;

import java.util.concurrent.TimeUnit;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

final class LatencyTracker implements LatencyMetrics {
    private static final Log LOG = LogFactory.getLog(LatencyTracker.class);
    private final Histogram histogram;

    public LatencyTracker(boolean concurrent) {
        this.histogram = concurrent
                ? new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(5L), 3)
                : new Histogram(TimeUnit.MINUTES.toNanos(5L), 3);
    }

    public void recordLatency(long latencyNanos) {
        try {
            histogram.recordValue(latencyNanos);
        } catch (Throwable t) {
            LOG.error("Failed recording value %d, ignoring it", latencyNanos, t);
        }
    }

    @Override
    public double getAvgLatency() {
        return histogram.getMean();
    }

    @Override
    public double getP99Latency() {
        return histogram.getValueAtPercentile(99);
    }

    @Override
    public double getP95Latency() {
        return histogram.getValueAtPercentile(95);
    }

    @Override
    public double getMaxLatency() {
        return histogram.getMaxValue();
    }

    @Override
    public double getPercentile(double p) {
        return histogram.getValueAtPercentile(p);
    }

    @Override
    public long getTotalMeasurements() {
        return histogram.getTotalCount();
    }
}
