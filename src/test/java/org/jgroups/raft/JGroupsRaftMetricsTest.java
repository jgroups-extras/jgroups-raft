package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.configuration.RuntimeProperties;
import org.jgroups.raft.metrics.ElectionMetrics;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.metrics.LogMetrics;
import org.jgroups.raft.metrics.PerformanceMetrics;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for the {@link JGroupsRaftMetrics} API.
 *
 * <p>
 * Verifies that all metric categories are accessible through the public API and reflect
 * the actual cluster state after operations. Uses a 3-node cluster with metrics enabled.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftMetricsTest {

    private static final int CLUSTER_SIZE = 3;

    private JRaftTestCluster<SimpleKVStateMachine> cluster;

    @BeforeClass
    public void setup() throws Exception {
        cluster = JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, CLUSTER_SIZE,
                builder -> builder.withRuntimeProperties(RuntimeProperties.from(
                        Map.of(JGroupsRaftMetrics.METRICS_ENABLED.name(), "true")
                )));
        cluster.waitUntilLeaderElected();
    }

    @AfterClass
    public void teardown() throws Exception {
        if (cluster != null)
            cluster.close();
    }

    // -- Cluster-level metrics --

    public void testTotalNodes() {
        JGroupsRaftMetrics metrics = cluster.leader().metrics();

        assertThat(metrics.getTotalNodes()).isEqualTo(CLUSTER_SIZE);
    }

    public void testActiveNodes() {
        JGroupsRaftMetrics metrics = cluster.leader().metrics();

        assertThat(metrics.getActiveNodes()).isEqualTo(CLUSTER_SIZE);
    }

    // -- Election metrics --

    public void testLeaderRaftIdIsSet() {
        JGroupsRaftMetrics metrics = cluster.leader().metrics();
        ElectionMetrics election = metrics.leaderMetrics();

        assertThat(election.getLeaderRaftId()).isNotNull().isNotEmpty();
    }

    public void testAllNodesAgreeOnLeader() {
        String expectedLeader = cluster.leader().metrics().leaderMetrics().getLeaderRaftId();

        for (int i = 0; i < CLUSTER_SIZE; i++) {
            ElectionMetrics election = cluster.raft(i).metrics().leaderMetrics();
            assertThat(election.getLeaderRaftId())
                    .as("Node %d should agree on leader", i)
                    .isEqualTo(expectedLeader);
        }
    }

    public void testTimeSinceLastElectionIsAccessible() {
        // The election happened before metrics were enabled (during channel connect),
        // so the election timestamps may not be recorded. We verify the metric is accessible
        // and returns a non-null duration on all nodes.
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            Duration d = cluster.raft(i).metrics().leaderMetrics().getTimeSinceLastLeaderChange();
            assertThat(d)
                    .as("Node %d should return a non-null duration", i)
                    .isNotNull();
        }
    }

    // -- Log metrics --

    public void testLogMetricsOnEmptyCluster() {
        LogMetrics log = cluster.leader().metrics().replicationMetrics();

        assertThat(log.getCurrentTerm()).isGreaterThan(0);
        assertThat(log.getSnapshotCount()).isGreaterThanOrEqualTo(0);
        assertThat(log.getSnapshotsReceived()).isGreaterThanOrEqualTo(0);
    }

    public void testLogMetricsAfterWrites() {
        JGroupsRaft<SimpleKVStateMachine> leader = cluster.leader();

        leader.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("m1", "v1"));
        leader.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("m2", "v2"));
        leader.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("m3", "v3"));

        LogMetrics leaderLog = leader.metrics().replicationMetrics();
        assertThat(leaderLog.getTotalLogEntries()).isGreaterThanOrEqualTo(3);
        assertThat(leaderLog.getCommittedLogEntries()).isGreaterThanOrEqualTo(3);
        assertThat(leaderLog.getUncommittedLogEntries()).isZero();
        assertThat(leaderLog.getLogSizeInBytes()).isGreaterThan(0);

        // Wait for followers to catch up.
        assertThat(eventually(() -> {
            for (int i = 0; i < CLUSTER_SIZE; i++) {
                LogMetrics log = cluster.raft(i).metrics().replicationMetrics();
                if (log.getCommittedLogEntries() < 3) return false;
            }
            return true;
        }, 10, TimeUnit.SECONDS)).isTrue();

        // All nodes should agree on term.
        long leaderTerm = leaderLog.getCurrentTerm();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertThat(cluster.raft(i).metrics().replicationMetrics().getCurrentTerm())
                    .as("Node %d should have same term as leader", i)
                    .isEqualTo(leaderTerm);
        }
    }

    // -- Performance metrics --

    public void testPerformanceMetricsAfterWrites() {
        JGroupsRaft<SimpleKVStateMachine> leader = cluster.leader();

        leader.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("p1", "v1"));
        leader.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("p2", "v2"));

        PerformanceMetrics perf = leader.metrics().performanceMetrics();

        // Leader should have total and processing latency recorded.
        LatencyMetrics total = perf.getTotalLatency();
        assertThat(total.getTotalMeasurements()).isGreaterThanOrEqualTo(2);
        assertThat(total.getAvgLatency()).isGreaterThan(0);
        assertThat(total.getMaxLatency()).isGreaterThanOrEqualTo(total.getAvgLatency());

        LatencyMetrics processing = perf.getProcessingLatency();
        assertThat(processing.getTotalMeasurements()).isGreaterThanOrEqualTo(2);
        assertThat(processing.getAvgLatency()).isGreaterThan(0);

        // Total should be >= processing (total includes queuing).
        assertThat(total.getAvgLatency()).isGreaterThanOrEqualTo(processing.getAvgLatency());
    }

    public void testFollowerRedirectLatency() {
        JGroupsRaft<SimpleKVStateMachine> follower = cluster.follower();

        follower.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("r1", "v1"));

        PerformanceMetrics perf = follower.metrics().performanceMetrics();
        LatencyMetrics redirect = perf.getRedirectLatency();

        assertThat(redirect.getTotalMeasurements()).isGreaterThanOrEqualTo(1);
        assertThat(redirect.getAvgLatency()).isGreaterThan(0);
    }

    public void testFollowerHasNoTotalOrProcessingLatency() {
        PerformanceMetrics perf = cluster.follower().metrics().performanceMetrics();

        assertThat(perf.getTotalLatency().getTotalMeasurements()).isZero();
        assertThat(perf.getProcessingLatency().getTotalMeasurements()).isZero();
    }

    // -- Disabled metrics --

    public void testDisabledMetricsReturnDefaults() {
        JGroupsRaftMetrics disabled = JGroupsRaftMetrics.disabled();

        assertThat(disabled.getTotalNodes()).isEqualTo(-1);
        assertThat(disabled.getActiveNodes()).isEqualTo(-1);

        ElectionMetrics election = disabled.leaderMetrics();
        assertThat(election.getLeaderRaftId()).isNull();
        assertThat(election.getLeaderElectionTime()).isEqualTo(Instant.EPOCH);
        assertThat(election.getTimeSinceLastLeaderChange()).isEqualTo(Duration.ZERO);

        LogMetrics log = disabled.replicationMetrics();
        assertThat(log.getTotalLogEntries()).isEqualTo(-1);
        assertThat(log.getCommittedLogEntries()).isEqualTo(-1);
        assertThat(log.getUncommittedLogEntries()).isEqualTo(-1);
        assertThat(log.getLogSizeInBytes()).isEqualTo(-1);
        assertThat(log.getCurrentTerm()).isEqualTo(-1);
        assertThat(log.getCommitIndex()).isEqualTo(-1);
        assertThat(log.getSnapshotCount()).isEqualTo(-1);
        assertThat(log.getSnapshotsReceived()).isEqualTo(-1);

        PerformanceMetrics perf = disabled.performanceMetrics();
        assertThat(perf.getTotalLatency().getTotalMeasurements()).isEqualTo(-1);
        assertThat(perf.getProcessingLatency().getTotalMeasurements()).isEqualTo(-1);
        assertThat(perf.getLeaderElectionLatency().getTotalMeasurements()).isEqualTo(-1);
        assertThat(perf.getRedirectLatency().getTotalMeasurements()).isEqualTo(-1);
    }
}
