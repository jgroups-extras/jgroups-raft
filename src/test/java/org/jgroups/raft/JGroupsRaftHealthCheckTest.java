package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.raft.JGroupsRaftHealthCheck.ClusterHealth;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftHealthCheckTest {

    private static final int CLUSTER_SIZE = 3;

    private JRaftTestCluster<SimpleKVStateMachine> cluster;

    @BeforeMethod
    public void setup() {
        cluster = JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, CLUSTER_SIZE);
        cluster.waitUntilLeaderElected();
    }

    @AfterMethod
    public void teardown() throws Exception {
        if (cluster != null)
            cluster.close();
    }

    public void testAllNodesLive() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertThat(cluster.raft(i).healthCheck().isNodeLive())
                    .as("Node %d should be live", i)
                    .isTrue();
        }
    }

    public void testAllNodesReady() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertThat(cluster.raft(i).healthCheck().isNodeReady())
                    .as("Node %d should be ready", i)
                    .isTrue();
        }
    }

    public void testHealthyCluster() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertThat(cluster.raft(i).healthCheck().getClusterHealth())
                    .as("Node %d should report HEALTHY", i)
                    .isEqualTo(ClusterHealth.HEALTHY);
        }
    }

    public void testDegradedAfterNodeDisconnect() throws Exception {
        // Disconnect a follower to bring active voting members below total but still at majority.
        JGroupsRaft<SimpleKVStateMachine> follower = cluster.follower();
        int followerIndex = -1;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (cluster.raft(i) == follower) {
                followerIndex = i;
                break;
            }
        }

        JChannel followerChannel = cluster.channel(followerIndex);
        followerChannel.disconnect();

        // Remaining nodes should eventually see DEGRADED.
        int leaderIndex = cluster.leaderIndex();
        assertThat(eventually(() -> cluster.raft(leaderIndex).healthCheck().getClusterHealth() == ClusterHealth.DEGRADED,
                10, TimeUnit.SECONDS)).isTrue();
    }

    public void testFailureAfterMajorityLost() throws Exception {
        // Disconnect two followers so the remaining node loses majority and the leader steps down.
        int leaderIndex = cluster.leaderIndex();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (i != leaderIndex) {
                cluster.channel(i).disconnect();
            }
        }

        JGroupsRaft<SimpleKVStateMachine> remaining = cluster.raft(leaderIndex);

        // Wait until the leader steps down (no majority).
        assertThat(eventually(() -> remaining.role() != JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();

        // Cluster health should be FAILURE.
        assertThat(eventually(() -> remaining.healthCheck().getClusterHealth() == ClusterHealth.FAILURE,
                10, TimeUnit.SECONDS)).isTrue();

        // Node should not be ready without a leader.
        assertThat(remaining.healthCheck().isNodeReady()).isFalse();

        // Node is still live (channel connected).
        assertThat(remaining.healthCheck().isNodeLive()).isTrue();
    }

    public void testLearnerNodeReportsWarning() throws Throwable {
        // Remove a follower from the Raft membership. It becomes a learner.
        JGroupsRaft<SimpleKVStateMachine> leader = cluster.leader();
        JGroupsRaft<SimpleKVStateMachine> follower = cluster.follower();
        String followerId = follower.state().id();

        leader.administration().removeNode(followerId)
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        // Wait until the removed node transitions to learner.
        assertThat(eventually(() -> follower.role() == JGroupsRaftRole.LEARNER, 10, TimeUnit.SECONDS)).isTrue();
    }

    public void testHealthCheckNeverNull() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertThat(cluster.raft(i).healthCheck())
                    .as("Node %d healthCheck() should never return null", i)
                    .isNotNull();
        }
    }
}
