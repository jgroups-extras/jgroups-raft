package org.jgroups.raft.internal.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import org.jgroups.Global;
import org.jgroups.raft.metrics.LatencyMetrics;

import org.testng.annotations.Test;

/**
 * Unit tests for {@link ElectionProtocolMetrics}.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ElectionProtocolMetricsTest {

    public void testInitialState() {
        ElectionProtocolMetrics metrics = new ElectionProtocolMetrics();

        assertThat(metrics.election().getTotalMeasurements()).isZero();
        assertThat(metrics.election().getAvgLatency()).isEqualTo(0.0);
    }

    public void testRecordElectionLatency() {
        ElectionProtocolMetrics metrics = new ElectionProtocolMetrics();

        metrics.recordElectionLatency(50_000_000);
        metrics.recordElectionLatency(150_000_000);

        LatencyMetrics election = metrics.election();
        assertThat(election.getTotalMeasurements()).isEqualTo(2);
        assertThat(election.getAvgLatency()).isBetween(80_000_000.0, 120_000_000.0);
        assertThat(election.getMaxLatency()).isCloseTo(150_000_000.0, withinPercentage(0.1));
    }

    public void testMeasurementsAccumulate() {
        ElectionProtocolMetrics metrics = new ElectionProtocolMetrics();

        int count = 10;
        for (int i = 1; i <= count; i++) {
            metrics.recordElectionLatency(i * 10_000_000L);
        }

        LatencyMetrics election = metrics.election();
        assertThat(election.getTotalMeasurements()).isEqualTo(count);
        assertThat(election.getMaxLatency()).isCloseTo(100_000_000.0, withinPercentage(0.1));
        assertThat(election.getAvgLatency()).isBetween(40_000_000.0, 70_000_000.0);
    }
}
