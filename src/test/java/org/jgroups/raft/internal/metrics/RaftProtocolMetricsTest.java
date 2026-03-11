package org.jgroups.raft.internal.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.metrics.LatencyMetrics;

import org.testng.annotations.Test;

/**
 * Unit tests for {@link RaftProtocolMetrics}.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RaftProtocolMetricsTest {

    public void testInitialState() {
        RaftProtocolMetrics metrics = new RaftProtocolMetrics();

        assertThat(metrics.total().getTotalMeasurements()).isZero();
        assertThat(metrics.total().getAvgLatency()).isEqualTo(0.0);

        assertThat(metrics.processing().getTotalMeasurements()).isZero();
        assertThat(metrics.processing().getAvgLatency()).isEqualTo(0.0);
    }

    public void testRecordTotalLatency() {
        RaftProtocolMetrics metrics = new RaftProtocolMetrics();

        metrics.recordTotalLatency(1_000_000);
        metrics.recordTotalLatency(3_000_000);

        LatencyMetrics total = metrics.total();
        assertThat(total.getTotalMeasurements()).isEqualTo(2);
        assertThat(total.getAvgLatency()).isBetween(1_500_000.0, 2_500_000.0);
        assertThat(total.getMaxLatency()).isGreaterThanOrEqualTo(3_000_000.0);

        // Processing should be unaffected.
        assertThat(metrics.processing().getTotalMeasurements()).isZero();
    }

    public void testRecordProcessingLatency() {
        RaftProtocolMetrics metrics = new RaftProtocolMetrics();

        metrics.recordProcessingLatency(500_000);
        metrics.recordProcessingLatency(1_500_000);

        LatencyMetrics processing = metrics.processing();
        assertThat(processing.getTotalMeasurements()).isEqualTo(2);
        assertThat(processing.getAvgLatency()).isBetween(500_000.0, 1_500_000.0);

        // Total should be unaffected.
        assertThat(metrics.total().getTotalMeasurements()).isZero();
    }

    public void testTrackersAreIndependent() {
        RaftProtocolMetrics metrics = new RaftProtocolMetrics();

        metrics.recordTotalLatency(10_000_000);
        metrics.recordProcessingLatency(5_000_000);

        assertThat(metrics.total().getTotalMeasurements()).isEqualTo(1);
        assertThat(metrics.processing().getTotalMeasurements()).isEqualTo(1);

        assertThat(metrics.total().getMaxLatency()).isGreaterThanOrEqualTo(10_000_000.0);
        assertThat(metrics.processing().getMaxLatency()).isGreaterThanOrEqualTo(5_000_000.0);
    }

    public void testPercentiles() {
        RaftProtocolMetrics metrics = new RaftProtocolMetrics();

        // Record a range of values.
        for (int i = 1; i <= 100; i++) {
            metrics.recordTotalLatency(i * 1_000_000L);
        }

        LatencyMetrics total = metrics.total();
        assertThat(total.getTotalMeasurements()).isEqualTo(100);

        // P50 should be around 50ms.
        assertThat(total.getPercentile(50)).isBetween(40_000_000.0, 60_000_000.0);

        // P99 should be around 99ms.
        assertThat(total.getP99Latency()).isGreaterThanOrEqualTo(90_000_000.0);

        // P95 should be around 95ms.
        assertThat(total.getP95Latency()).isGreaterThanOrEqualTo(85_000_000.0);
    }
}
