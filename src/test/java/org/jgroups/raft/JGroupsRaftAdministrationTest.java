package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.util.Util;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftAdministrationTest {

    private JRaftTestCluster<SimpleKVStateMachine> cluster;

    @BeforeClass
    protected void beforeClass() {
        cluster = JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, 3);
        cluster.waitUntilLeaderElected();
    }

    @AfterClass
    protected void afterClass() throws Throwable {
        cluster.close();
    }

    public void testAddAndRemoveNewMember() throws Throwable {
        // Initial members should contain all the configured members.
        Set<String> initialMembers = cluster.raft(0).administration().members();
        assertThat(initialMembers).hasSize(3);

        // Add a new member to the cluster.
        cluster.raft(0).administration().addNode("newMember")
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        Set<String> updatedMembers = cluster.raft(0).administration().members();
        assertThat(updatedMembers).hasSize(4);
        assertThat(updatedMembers).contains("newMember");

        // Remove the new member from the cluster.
        cluster.raft(0).administration().removeNode("newMember")
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        Set<String> finalMembers = cluster.raft(0).administration().members();
        assertThat(finalMembers).hasSize(3);
        assertThat(finalMembers).doesNotContain("newMember");
        assertThat(finalMembers).isEqualTo(initialMembers);
    }

    public void testSnapshot() throws Throwable {
        JGroupsRaft<SimpleKVStateMachine> leader = cluster.leader();

        // Write some data so there's something to snapshot.
        leader.write(sm -> sm.handlePut("key1", "value1"));

        // Trigger snapshot via the administration API.
        leader.administration().snapshot()
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        // Verify the snapshot was created.
        int leaderIndex = cluster.leaderIndex();
        RAFT raft = cluster.raftProtocol(leaderIndex);
        assertThat(raft.numSnapshots()).isPositive();
    }

    public void testForceLeaderElection() throws Throwable {
        String oldLeader = cluster.leader().state().leader();

        // raft(0) is the coordinator (first channel created, first in view).
        String newLeader = cluster.raft(0).administration().forceLeaderElection()
                .toCompletableFuture().get(30, TimeUnit.SECONDS);

        assertThat(newLeader).isNotNull();
        assertThat(newLeader).isNotEqualTo(oldLeader);

        cluster.waitUntilLeaderElected();
    }

    public void testForceLeaderElectionAfterOperations() throws Throwable {
        cluster.waitUntilLeaderElected();
        JGroupsRaft<SimpleKVStateMachine> leader = cluster.leader();
        String oldLeader = leader.state().leader();

        // Submit operations to create different log indexes across nodes.
        for (int i = 0; i < 10; i++) {
            String k = "k-" + i;
            String v = "v-" + i;
            leader.write(sm -> sm.handlePut(k, v));
        }

        String newLeader = cluster.raft(0).administration().forceLeaderElection()
                .toCompletableFuture().get(30, TimeUnit.SECONDS);

        assertThat(newLeader).isNotNull();
        assertThat(newLeader).isNotEqualTo(oldLeader);

        cluster.waitUntilLeaderElected();
    }

    public void testForceLeaderElectionOnNonCoordinator() {
        // raft(1) is never the coordinator.
        assertThatThrownBy(() -> cluster.raft(1).administration().forceLeaderElection()
                .toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    public void testForceLeaderElectionCancellation() throws Exception {
        // Create a separate cluster since we need to stop nodes for reliable cancellation.
        JRaftTestCluster<SimpleKVStateMachine> isolated =
                JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, 3);
        try {
            isolated.waitUntilLeaderElected();

            // Close 2 channels so elections can never reach majority.
            Util.close(isolated.channel(1));
            Util.close(isolated.channel(2));
            Util.waitUntilAllChannelsHaveSameView(10_000, 100, isolated.channel(0));

            // Force election through the v2 API, election loops without majority and never complete.
            var future = isolated.raft(0).administration().forceLeaderElection()
                    .toCompletableFuture();

            // Cancel the election.
            boolean cancelled = future.cancel(true);

            assertThat(cancelled).isTrue();
            assertThat(future).isCancelled();

            // Verify the voting thread stops after cancellation is detected.
            BaseElection election = RaftTestUtils.election(isolated.channel(0));
            assertThat(eventually(() -> !election.isVotingThreadRunning(), 10, TimeUnit.SECONDS)).isTrue();
        } finally {
            isolated.close();
        }
    }

}
