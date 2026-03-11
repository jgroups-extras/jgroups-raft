package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.internal.metrics.RedirectProtocolMetrics;
import org.jgroups.raft.tests.harness.BaseStateMachineTest;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.util.Bits;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

/**
 * Integration test for REDIRECT protocol metrics collection.
 *
 * <p>
 * Creates a real 2-node cluster with stats enabled and verifies that redirect operations
 * on followers correctly record round-trip latency in {@link RedirectProtocolMetrics},
 * while the leader's REDIRECT records nothing.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RedirectMetricsTest extends BaseStateMachineTest<CounterStateMachine> {

    {
        clusterSize = 2;
        recreatePerMethod = true;
    }

    @Override
    protected CounterStateMachine createStateMachine(JChannel ch) {
        return new CounterStateMachine();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.resendInterval(600_000).maxLogSize(1_000_000);
        raft.enableStats(true);
    }

    @Override
    protected void afterClusterCreation() throws Exception {
        super.afterClusterCreation();
        raft(0).setLeaderAndTerm(address(0));
        raft(1).setLeaderAndTerm(address(0));
    }

    /**
     * A write submitted through a follower records exactly 1 redirect measurement on that follower.
     */
    public void testFollowerRedirectRecordsMeasurement() throws Exception {
        RedirectProtocolMetrics followerMetrics = redirectMetrics(1);

        // Submit through follower — triggers redirect to leader.
        addValue(handle(1), 42);

        assertThat(followerMetrics.redirect().getTotalMeasurements())
                .as("Follower should record 1 redirect measurement")
                .isEqualTo(1);
    }

    /**
     * The leader handles requests locally, so its REDIRECT records no measurements.
     */
    public void testLeaderRedirectRecordsNothing() throws Exception {
        RedirectProtocolMetrics leaderMetrics = redirectMetrics(0);

        // Submit through leader — handled locally, no redirect.
        addValue(handle(0), 42);

        assertThat(leaderMetrics.redirect().getTotalMeasurements())
                .as("Leader should have 0 redirect measurements")
                .isZero();
    }

    /**
     * Multiple redirects from the follower accumulate the correct count.
     */
    public void testMultipleRedirectsAccumulate() throws Exception {
        RedirectProtocolMetrics followerMetrics = redirectMetrics(1);

        int count = 5;
        for (int i = 0; i < count; i++) {
            addValue(handle(1), i + 1);
        }

        assertThat(followerMetrics.redirect().getTotalMeasurements())
                .as("Follower should record %d redirect measurements", count)
                .isEqualTo(count);
    }

    /**
     * resetStats() clears redirect metrics.
     */
    public void testResetStatsClearsRedirectMetrics() {
        RedirectProtocolMetrics before = redirectMetrics(1);

        // Record directly — wiring is verified by testFollowerRedirectRecordsMeasurement.
        before.recordRedirectLatency(1_000_000);
        assertThat(before.redirect().getTotalMeasurements()).isEqualTo(1);

        REDIRECT redirect = redirect(1);
        redirect.resetStats();

        RedirectProtocolMetrics after = redirectMetrics(1);
        assertThat(after).as("resetStats should create a new metrics instance").isNotSameAs(before);
        assertThat(after.redirect().getTotalMeasurements()).isZero();
    }

    /**
     * Metrics disabled returns -1 from the JMX attribute.
     */
    public void testMetricsDisabled() {
        REDIRECT redirect = redirect(1);
        raft(1).enableStats(false);
        redirect.resetStats();

        assertThat(redirect.redirectionOperationMeanLatency()).isEqualTo(-1);
    }

    private REDIRECT redirect(int index) {
        return channel(index).getProtocolStack().findProtocol(REDIRECT.class);
    }

    private RedirectProtocolMetrics redirectMetrics(int index) {
        REDIRECT redirect = redirect(index);

        try {
            Field field = REDIRECT.class.getDeclaredField("metrics");
            field.setAccessible(true);
            RedirectProtocolMetrics metrics = (RedirectProtocolMetrics) field.get(redirect);
            assertThat(metrics).as("REDIRECT metrics should not be null when stats enabled").isNotNull();
            return metrics;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to access REDIRECT.metrics field", e);
        }
    }

    private static void addValue(RaftHandle rh, int delta) throws Exception {
        byte[] val = new byte[Integer.BYTES];
        Bits.writeInt(delta, val, 0);
        byte[] retval = rh.set(val, 0, val.length, 5, TimeUnit.SECONDS);
        Bits.readInt(retval, 0);
    }
}
