package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.tests.harness.BaseRaftChannelTest;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.tests.harness.Helper.assertCommitIndex;
import static org.jgroups.tests.harness.Helper.waitUntilAllRaftsHaveLeader;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class MaintenanceClusterTest extends BaseRaftChannelTest {

    {
        createManually = true;
        recreatePerMethod = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }

    public void testMaintenanceWorkflow() throws Exception {
        // A cluster is created with all the three nodes.
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        // The existing cluster operates just as usual.
        insertEntries();

        // After a certain point, a node needs to be removed for maintenance.
        // The leader will remove it.
        RAFT leader = leader();
        int removeIndex = followerIndex();
        RAFT node = raft(removeIndex);

        leader.removeServer(node.raftId()).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> node.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        // After the node is removed and become learner, the cluster continues to operate.
        insertEntries();

        // Until, the operator decides to stop the removed node.
        Util.close(channel(removeIndex));
        channels()[removeIndex] = null;
        Util.waitUntilAllChannelsHaveSameView(10_000, 150, actualChannels());

        // With the node removed for maintenance, the cluster still operates correctly.
        insertEntries();

        // Maintenance is completed and the node is started again.
        // It should connect to the cluster and start as learner.
        createCluster();
        RAFT restarted = raft(removeIndex);
        assertThat(restarted.role()).isEqualTo("Learner");
        assertThat(leader.members()).hasSize(2).doesNotContain(restarted.raftId());

        // Eventually the learner will catch up with the cluster.
        assertCommitIndex(10_000, leader.lastAppended(), leader.lastAppended(), this::raft, channel(removeIndex));

        // The node is added again as a Raft member.
        leader.addServer(restarted.raftId()).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> restarted.role().equals("Follower"), 10, TimeUnit.SECONDS)).isTrue();

        // And the cluster continues to operate.
        insertEntries();
    }

    private int followerIndex() {
        JChannel[] channels = channels();
        for (int i = 0; i < channels.length; i++) {
            RAFT r = raft(channels[i]);
            if (!r.isLeader())
                return i;
        }

        throw new AssertionError("Follower not found");
    }

    private void insertEntries() throws Exception {
        RAFT leader = leader();

        byte[] payload = new byte[] { 1 };
        for (int i = 0; i < 16; i++) {
            leader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        }

        assertCommitIndex(10_000, leader.lastAppended(), leader.lastAppended(), this::raft, actualChannels());
    }
}
