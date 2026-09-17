package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.internal.metrics.RaftProtocolMetrics;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.tests.harness.BaseStateMachineTest;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

/**
 * Integration test for RAFT protocol metrics collection.
 *
 * <p>
 * Creates a real 2-node cluster with stats enabled and verifies that write operations
 * correctly wire into the {@link RaftProtocolMetrics} — recording both total and processing
 * latency with the expected measurement counts.
 * </p>
 *
 * <p>
 * Latency values are not asserted exactly since real time is used; the unit test
 * {@code DownRequestTrackingTest} covers exact latency verification with controlled time.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RaftMetricsTest extends BaseStateMachineTest<CounterStateMachine> {

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
    protected Protocol[] baseProtocolStackForNode(String name) {
        return Util.getTestStack(createNewRaft(name), createRedirect());
    }

    @Override
    protected void afterClusterCreation() throws Exception {
        super.afterClusterCreation();
        raft(0).setLeaderAndTerm(address(0), 20);
        raft(1).setLeaderAndTerm(address(0), 20);
    }

    /**
     * A single write records exactly 1 total and 1 processing measurement on the leader.
     */
    public void testSingleWriteRecordsMeasurements() throws Exception {
        RaftProtocolMetrics metrics = leaderMetrics();

        addValue(handle(0), 42);

        assertThat(metrics.total().getTotalMeasurements())
                .as("Single write should record exactly 1 total measurement")
                .isEqualTo(1);
        assertThat(metrics.processing().getTotalMeasurements())
                .as("Single write should record exactly 1 processing measurement")
                .isEqualTo(1);
    }

    /**
     * Multiple writes accumulate the correct measurement count.
     */
    public void testMultipleWritesAccumulateMeasurements() throws Exception {
        RaftProtocolMetrics metrics = leaderMetrics();

        int writeCount = 10;
        for (int i = 0; i < writeCount; i++) {
            addValue(handle(0), i + 1);
        }

        assertThat(metrics.total().getTotalMeasurements())
                .as("Should record %d total measurements", writeCount)
                .isEqualTo(writeCount);
        assertThat(metrics.processing().getTotalMeasurements())
                .as("Should record %d processing measurements", writeCount)
                .isEqualTo(writeCount);
    }

    /**
     * Total latency is always greater than or equal to processing latency
     * because total includes queue wait and state machine apply time.
     */
    public void testTotalLatencyCoversProcessing() throws Exception {
        RaftProtocolMetrics metrics = leaderMetrics();

        for (int i = 0; i < 5; i++) {
            addValue(handle(0), i + 1);
        }

        LatencyMetrics total = metrics.total();
        LatencyMetrics processing = metrics.processing();

        assertThat(total.getAvgLatency())
                .as("Total avg latency should be >= processing avg latency")
                .isGreaterThanOrEqualTo(processing.getAvgLatency());
        assertThat(total.getMaxLatency())
                .as("Total max latency should be >= processing max latency")
                .isGreaterThanOrEqualTo(processing.getMaxLatency());
    }

    /**
     * Follower nodes do not record any metrics since they don't submit requests.
     */
    public void testFollowerRecordsNoMeasurements() throws Exception {
        RaftProtocolMetrics followerMetrics = followerMetrics();

        addValue(handle(0), 1);
        addValue(handle(0), 2);

        assertThat(followerMetrics.total().getTotalMeasurements())
                .as("Follower should have 0 total measurements")
                .isZero();
        assertThat(followerMetrics.processing().getTotalMeasurements())
                .as("Follower should have 0 processing measurements")
                .isZero();
    }

    /**
     * Metrics are null when stats are disabled and JMX returns -1.
     */
    public void testMetricsDisabled() {
        RAFT leader = raft(0);
        leader.enableStats(false);
        leader.resetStats();

        assertThat(leader.userOperationMeanLatency()).isEqualTo(-1);
        assertThat(leader.replicationMeanLatency()).isEqualTo(-1);
    }

    /**
     * resetStats() creates fresh metrics, clearing previous recordings.
     */
    public void testResetStatsClearsMetrics() throws Exception {
        RaftProtocolMetrics before = leaderMetrics();

        addValue(handle(0), 1);
        assertThat(before.total().getTotalMeasurements()).isEqualTo(1);

        RAFT leader = raft(0);
        leader.resetStats();

        RaftProtocolMetrics after = leaderMetrics();
        assertThat(after).as("resetStats should create a new metrics instance").isNotSameAs(before);
        assertThat(after.total().getTotalMeasurements()).isZero();
        assertThat(after.processing().getTotalMeasurements()).isZero();
    }

    /**
     * Log state reflects the cluster after writes complete.
     */
    public void testLogStateAfterWrites() throws Exception {
        RAFT leader = raft(0);
        leader.sendCommitsImmediately(true);

        int writeCount = 5;
        for (int i = 0; i < writeCount; i++) {
            addValue(handle(0), i + 1);
        }

        assertThat(eventually(() -> leader.lastAppended() == leader.commitIndex(), 10, TimeUnit.SECONDS)).isTrue();

        assertThat(leader.log().size())
                .as("Leader should have at least %d log entries", writeCount)
                .isGreaterThanOrEqualTo(writeCount);
        assertThat(leader.lastAppended())
                .as("All entries should be committed")
                .isEqualTo(leader.commitIndex());
        assertThat(leader.currentTerm())
                .isGreaterThan(0);
        assertThat(leader.currentLogSize())
                .isGreaterThan(0);

        // Follower should eventually converge to the same committed state.
        RAFT follower = raft(1);
        long leaderCommit = leader.commitIndex();
        assertThat(eventually(() -> follower.commitIndex() >= leaderCommit, 10, TimeUnit.SECONDS))
                .as("Follower commit index should catch up to leader's (%d)", leaderCommit)
                .isTrue();
        assertThat(follower.currentTerm())
                .isEqualTo(leader.currentTerm());
    }

    private RaftProtocolMetrics leaderMetrics() {
        // Accesses the package-private field via the JMX wrapper.
        // If metrics is null, the test setup is wrong.
        RAFT leader = raft(0);
        assertThat(leader.isLeader()).isTrue();

        // Use reflection to access the private field since there's no public getter yet.
        try {
            Field field = RAFT.class.getDeclaredField("metrics");
            field.setAccessible(true);
            RaftProtocolMetrics metrics = (RaftProtocolMetrics) field.get(leader);
            assertThat(metrics).as("Leader metrics should not be null when stats enabled").isNotNull();
            return metrics;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to access RAFT.metrics field", e);
        }
    }

    private RaftProtocolMetrics followerMetrics() {
        RAFT follower = raft(1);
        assertThat(follower.isLeader()).isFalse();

        try {
            var field = RAFT.class.getDeclaredField("metrics");
            field.setAccessible(true);
            RaftProtocolMetrics metrics = (RaftProtocolMetrics) field.get(follower);
            assertThat(metrics).as("Follower metrics should not be null when stats enabled").isNotNull();
            return metrics;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to access RAFT.metrics field", e);
        }
    }

    private static void addValue(RaftHandle rh, int delta) throws Exception {
        byte[] val = new byte[Integer.BYTES];
        Bits.writeInt(delta, val, 0);
        byte[] retval = rh.set(val, 0, val.length, 5, TimeUnit.SECONDS);
        Bits.readInt(retval, 0);
    }
}
