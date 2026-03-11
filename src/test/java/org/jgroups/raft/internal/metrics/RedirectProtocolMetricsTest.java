package org.jgroups.raft.internal.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import org.jgroups.Global;
import org.jgroups.raft.metrics.LatencyMetrics;

import org.testng.annotations.Test;

/**
 * Unit tests for {@link RedirectProtocolMetrics}.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RedirectProtocolMetricsTest {

    public void testInitialState() {
        RedirectProtocolMetrics metrics = new RedirectProtocolMetrics();

        assertThat(metrics.redirect().getTotalMeasurements()).isZero();
        assertThat(metrics.redirect().getAvgLatency()).isEqualTo(0.0);
    }

    public void testRecordRedirectLatency() {
        RedirectProtocolMetrics metrics = new RedirectProtocolMetrics();

        metrics.recordRedirectLatency(2_000_000);
        metrics.recordRedirectLatency(6_000_000);

        LatencyMetrics redirect = metrics.redirect();
        assertThat(redirect.getTotalMeasurements()).isEqualTo(2);
        assertThat(redirect.getAvgLatency()).isBetween(3_000_000.0, 5_000_000.0);
        assertThat(redirect.getMaxLatency()).isCloseTo(6_000_000.0, withinPercentage(0.1));
    }

    public void testMeasurementsAccumulate() {
        RedirectProtocolMetrics metrics = new RedirectProtocolMetrics();

        int count = 25;
        for (int i = 1; i <= count; i++) {
            metrics.recordRedirectLatency(i * 1_000_000L);
        }

        LatencyMetrics redirect = metrics.redirect();
        assertThat(redirect.getTotalMeasurements()).isEqualTo(count);
        assertThat(redirect.getMaxLatency()).isCloseTo(25_000_000.0, withinPercentage(0.1));
        assertThat(redirect.getAvgLatency()).isBetween(10_000_000.0, 16_000_000.0);
    }
}
