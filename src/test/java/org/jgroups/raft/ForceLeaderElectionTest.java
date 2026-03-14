package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.raft.tests.harness.RaftAssertion.assertCommitIndex;
import static org.jgroups.raft.tests.harness.RaftAssertion.waitUntilAllRaftsHaveLeader;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.raft.tests.harness.BaseRaftChannelTest;
import org.jgroups.util.Util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ForceLeaderElectionTest extends BaseRaftChannelTest {

    {
        createManually = true;
        recreatePerMethod = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }

    /**
     * Start 2 of 3 nodes, submit operations, then start the 3rd node with an empty log.
     * Force election immediately. The 3rd node has a gap but one of the original followers
     * should be electable since it has the same log as the excluded leader.
     */
    public void testForceElectionWithLateJoiner() throws Exception {
        // Configure 3-node membership but only start 2 nodes.
        withClusterSize(3);
        createCluster(2);

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        // Submit operations so the 2 running nodes have entries the 3rd doesn't.
        RAFT leader = leader();
        byte[] payload = new byte[] { 1 };
        for (int i = 0; i < 16; i++) {
            leader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        }

        assertCommitIndex(10_000, leader.lastAppended(), leader.lastAppended(), this::raft, actualChannels());

        // Start the 3rd node, it joins with an empty log. Do NOT wait for catch-up.
        createCluster();

        // Force election on the coordinator, excluding the current leader.
        RAFT oldLeader = leader();
        String oldLeaderRaftId = oldLeader.raftId();
        BaseElection election = RaftTestUtils.election(channel(0));

        var result = election.startForcedElection(oldLeader.getAddress())
                .toCompletableFuture().get(30, TimeUnit.SECONDS);

        assertThat(result).isNotNull();
        assertThat(result).isNotEqualTo(oldLeader.getAddress());

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        // The new leader should be different.
        RAFT newLeader = leader();
        assertThat(newLeader.raftId()).isNotEqualTo(oldLeaderRaftId);
    }

    /**
     * Start a 3-node cluster, close 2 nodes so the election can never reach majority, then cancel the future.
     * The voting thread should stop.
     */
    public void testForceElectionCancellation() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        // Close 2 channels so the election can never reach majority.
        Util.close(channel(1));
        Util.close(channel(2));
        Util.waitUntilAllChannelsHaveSameView(10_000, 100, channel(0));

        BaseElection election = RaftTestUtils.election(channel(0));

        // Use a short vote_timeout so each round completes quickly, but the election still can't succeed (only 1 of 3 nodes).
        election.voteTimeout(1);

        CompletableFuture<Address> future = election.startForcedElection(null)
                .toCompletableFuture();

        // Election is looping, can't reach majority. Cancel it.
        boolean cancelled = future.cancel(true);

        assertThat(cancelled).isTrue();
        assertThat(future).isCancelled();

        // Verify the voting thread stops after cancellation is detected.
        assertThat(eventually(() -> !election.isVotingThreadRunning(), 10, TimeUnit.SECONDS)).isTrue();
    }
}
