package org.jgroups.raft.internal.metrics;

import org.jgroups.raft.metrics.LatencyMetrics;

import java.util.concurrent.TimeUnit;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

public final class LatencyTracker implements LatencyMetrics {
    private final Histogram histogram;

    public LatencyTracker() {
        this.histogram = new ConcurrentHistogram(TimeUnit.MINUTES.toNanos(5L), 5);
    }

    public void recordLatency(long latencyNanos) {
        try {
            histogram.recordValue(latencyNanos);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
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
