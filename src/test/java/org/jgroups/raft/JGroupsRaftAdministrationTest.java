package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;

import java.util.Set;
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

    public void testForceLeaderElection() {
        assertThatThrownBy(() -> cluster.raft(0).administration().forceLeaderElection())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
