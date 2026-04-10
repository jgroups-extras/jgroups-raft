package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.raft.tests.harness.RaftAssertion.assertCommitIndex;
import static org.jgroups.raft.tests.harness.RaftAssertion.assertLeaderlessOperationThrows;
import static org.jgroups.raft.tests.harness.RaftAssertion.waitUntilAllRaftsHaveLeader;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.tests.harness.BaseRaftChannelTest;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LearnerMemberTest extends BaseRaftChannelTest {

    {
        createManually = true;
        recreatePerMethod = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }

    public void testStartAsLearner() throws Exception {
        withClusterSize(3);
        createCluster();

        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            RAFT leaner = raft(ch);
            assertThat(leaner.role()).isEqualTo("Learner");
        }
    }

    public void testLearnerDoesNotBecomeLeader() throws Exception {
        withClusterSize(3);

        JChannel o = null;
        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            // Outside node correctly starts as a learner.
            RAFT learner = raft(ch);
            assertThat(learner.role()).isEqualTo("Learner");

            // A Raft member starts.
            o = createChannel("A");
            RAFT other = raft(o);

            // The learner node is allowed to run the election mechanism.
            // However, it should never vote nor become the leader.
            BaseElection be = RAFT.findProtocol(BaseElection.class, learner, true);
            Util.waitUntilAllChannelsHaveSameView(10_000, 150, ch, o);
            assertThat(eventually(() -> !be.isVotingThreadRunning(), 10, TimeUnit.SECONDS)).isTrue();

            // Verify the learner is still a learner and the other node wasn't elected because a majority isn't reached.
            assertThat(learner.role()).isEqualTo("Learner");
            assertThat(other.isLeader()).isFalse();
        } finally {
            Util.close(o);
        }
    }

    public void testLearnerExecuteElectionProcedure() throws Exception {
        JChannel[] cluster = new JChannel[3];
        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            // After the learner is the JGroups coordinator, we start the remaining nodes.
            for (int i = 0; i < cluster.length; i++) {
                cluster[i] = createChannel(Character.toString('A' + i));
            }

            Util.waitUntilAllChannelsHaveSameView(10_000, 250, ch, cluster[0], cluster[1], cluster[2]);
            waitUntilAllRaftsHaveLeader(cluster, this::raft);
        } finally {
            Arrays.stream(cluster).forEach(Util::close);
        }
    }

    public void testLearnerDoesNotReachMajority() throws Exception {
        withClusterSize(3);
        createCluster(1);

        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            RAFT learner = raft(ch);

            // Verify the learner is still a learner and the other node wasn't elected because a majority isn't reached.
            assertThat(learner.role()).isEqualTo("Learner");

            RAFT other = raft(0);
            Util.waitUntilAllChannelsHaveSameView(10_000, 150, channel(0), ch);
            BaseElection be = RAFT.findProtocol(BaseElection.class, other, true);
            assertThat(eventually(() -> !be.isVotingThreadRunning(), 10, TimeUnit.SECONDS)).isTrue();
            assertThat(other.isLeader()).isFalse();
        }
    }

    public void testMajorityLostWithOnlyLearner() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            List<Integer> followers = new ArrayList<>();
            int leader = -1;
            JChannel[] channels = channels();
            for (int i = 0; i < channels.length; i++) {
                RAFT r = raft(channels[i]);
                if (!r.isLeader())
                    followers.add(i);
                else leader = i;
            }

            for (Integer follower : followers) {
                close(follower);
            }

            Util.waitUntilAllChannelsHaveSameView(10_000, 150, channels[leader], ch);

            RAFT l = raft(leader);
            assertThat(eventually(() -> !l.isLeader(), 10, TimeUnit.SECONDS)).isTrue();

            RAFT learner = raft(ch);
            assertThat(learner.role()).isEqualTo("Learner");
            assertThat(l.role()).isEqualTo("Follower");
            assertThat(learner.leader()).isNull();
        }
    }

    public void testOperatingWithManyFollowers() throws Exception {
        withClusterSize(3);
        createCluster();
        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        List<JChannel> learners = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            learners.add(createChannel(UUID.randomUUID().toString()));
        }

        List<JChannel> allChannels = new ArrayList<>();
        allChannels.addAll(List.of(channels()));
        allChannels.addAll(learners);
        Util.waitUntilAllChannelsHaveSameView(10_000, 250, allChannels);

        RAFT leader = leader();
        byte[] payload = new byte[] { 0x4 };
        for (int i = 0; i < 32; i++) {
            leader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        }

        assertCommitIndex(20_000, leader.lastAppended(), leader.lastAppended(), this::raft, allChannels);

        // Stop all learners and it still operates correctly.
        learners.forEach(Util::close);

        Util.waitUntilAllChannelsHaveSameView(10_000, 150, channels());
        assertThat(leader.isLeader()).isTrue();

        for (int i = 0; i < 16; i++) {
            leader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        }

        assertCommitIndex(20_000, leader.lastAppended(), leader.lastAppended(), this::raft, channels());
    }

    public void testSubmitOperationsFromLearner() throws Exception {
        withClusterSize(3);
        createCluster();
        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            RAFT learner = raft(ch);

            assertThat(learner.role()).isEqualTo("Learner");
            assertThat(eventually(() -> learner.leader() != null, 10, TimeUnit.SECONDS)).isTrue();

            RaftHandle learnerHandle = new RaftHandle(ch, null);

            byte[] payload = new byte[] { 0x3 };
            for (int i = 0; i < 16; i++) {
                learnerHandle.set(payload, 0, 1, 10, TimeUnit.SECONDS);
            }

            List<JChannel> channels = new ArrayList<>();
            channels.add(ch);
            channels.addAll(List.of(channels()));

            assertCommitIndex(10_000, 16, 16, this::raft, channels);
        }
    }

    public void testLearnerNodeReceiveEntries() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            RAFT leader = leader();
            leader.set(new byte[] { 0x0 }, 0, 1, 10, TimeUnit.SECONDS);

            assertCommitIndex(10_000, leader.lastAppended(), leader.lastAppended(), this::raft, channel(0), ch);
        }
    }

    public void testLearnerNodeCatchUp() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT leader = leader();

        byte[] payload = new byte[] { 0x0 };
        for (int i = 0; i < 32; i++) {
            leader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        }

        // Wait until everyone catches up.
        assertCommitIndex(10_000, leader.lastAppended(), leader.lastAppended(), this::raft, channels());

        // Start learner and verify it will catch up with leader.
        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            assertCommitIndex(10_000, leader.lastAppended(), leader.lastAppended(), this::raft, channel(0), ch);
        }
    }

    public void testLearnerBecomesFollower() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT leader = leader();

        // The list only includes the 3 expected members.
        assertThat(leader.members()).hasSize(3).containsExactlyElementsOf(getRaftMembers());

        try (JChannel ch = createChannel(UUID.randomUUID().toString())) {
            RAFT learner = raft(ch);
            assertThat(learner.role()).isEqualTo("Learner");

            // Promotes the learner, it should be included in the member list in all nodes and should become Follower.
            leader.addServer(learner.raftId()).get(10, TimeUnit.SECONDS);
            assertThat(eventually(() -> learner.members().contains(learner.raftId()), 10, TimeUnit.SECONDS)).isTrue();

            assertThat(learner.role()).isEqualTo("Follower");

            for (JChannel member : channels()) {
                RAFT r = raft(member);
                assertThat(r.members()).hasSize(4).contains(learner.raftId());
            }
        }
    }

    public void testFollowerBecomesLearner() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        RAFT follower = follower();

        assertThat(follower.role()).isEqualTo("Follower");
        assertThat(leader.members()).hasSize(3).contains(follower.raftId());

        // Remove the follower node. It should become a learner and removed from the member list in all nodes.
        leader.removeServer(follower.raftId()).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> follower.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        for (JChannel channel : channels()) {
            RAFT r = raft(channel);
            assertThat(r.members()).hasSize(2).doesNotContain(follower.raftId());
        }
    }

    public void testLeaderBecomesLearner() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();

        assertThat(leader.isLeader()).isTrue();

        leader.removeServer(leader.raftId()).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        BooleanSupplier leaderRemoved = () -> Arrays.stream(channels())
                .map(this::raft)
                .allMatch(r -> r.members().size() == 2);
        assertThat(eventually(leaderRemoved, 10, TimeUnit.SECONDS)).isTrue();

        for (JChannel channel : channels()) {
            RAFT r = raft(channel);
            assertThat(r.members()).hasSize(2).doesNotContain(leader.raftId());
        }

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT after = leader();

        assertThat(after.raftId()).isNotEqualTo(leader.raftId());
    }

    public void testLeaderSelfRemovalFromTwoNodeCluster() throws Exception {
        withClusterSize(2);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        String removedRaftId = leader.raftId();

        leader.removeServer(removedRaftId).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        BooleanSupplier membersUpdated = () -> Arrays.stream(channels())
                .map(this::raft)
                .allMatch(r -> r.members().size() == 1);
        assertThat(eventually(membersUpdated, 10, TimeUnit.SECONDS)).isTrue();

        for (JChannel channel : channels()) {
            RAFT r = raft(channel);
            assertThat(r.members()).hasSize(1).doesNotContain(removedRaftId);
        }

        // With a single remaining member (majority = 1), a new leader should be elected.
        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT newLeader = leader();
        assertThat(newLeader.raftId()).isNotEqualTo(removedRaftId);
    }

    public void testLeaderSelfRemovalWithFollowerDown() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        String removedRaftId = leader.raftId();

        int downIndex = indexOf(false);
        Util.close(channel(downIndex));
        channels()[downIndex] = null;
        Util.waitUntilAllChannelsHaveSameView(10_000, 150, actualChannels());

        // C_old majority = 2, achieved with leader's vote + remaining follower's ack.
        leader.removeServer(removedRaftId).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        // C_new majority = 2, but only one member is alive. No leader can be elected.
        RAFT remaining = Arrays.stream(actualChannels())
                .map(this::raft)
                .filter(r -> !r.raftId().equals(removedRaftId))
                .findFirst()
                .orElseThrow();
        BaseElection be = RAFT.findProtocol(BaseElection.class, remaining, true);
        assertThat(eventually(() -> !be.isVotingThreadRunning(), 10, TimeUnit.SECONDS)).isTrue();
        assertThat(remaining.isLeader()).isFalse();
        assertThat(leader()).isNull();

        // Without a leader, operations on the remaining member should be rejected.
        assertLeaderlessOperationThrows(() -> remaining.set(new byte[] { 1 }, 0, 1, 500, TimeUnit.MILLISECONDS));

        // Restart the downed follower to restore majority.
        createCluster();
        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT newLeader = leader();
        assertThat(newLeader).isNotNull();
        assertThat(newLeader.raftId()).isNotEqualTo(removedRaftId);
    }

    public void testLeaderSelfRemovalThenChannelClose() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        String removedRaftId = leader.raftId();
        int leaderIndex = indexOf(true);

        leader.removeServer(removedRaftId).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        close(leaderIndex);
        Util.waitUntilAllChannelsHaveSameView(10_000, 150, actualChannels());

        waitUntilAllRaftsHaveLeader(actualChannels(), this::raft);
        RAFT newLeader = leader();
        assertThat(newLeader).isNotNull();
        assertThat(newLeader.raftId()).isNotEqualTo(removedRaftId);

        byte[] payload = new byte[] { 1 };
        newLeader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        assertCommitIndex(10_000, newLeader.lastAppended(), newLeader.lastAppended(), this::raft, actualChannels());
    }

    public void testRemoveAlreadyRemovedNode() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        String removedRaftId = leader.raftId();

        leader.removeServer(removedRaftId).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT newLeader = leader();

        // Removing the same node again should complete without error.
        newLeader.removeServer(removedRaftId).get(10, TimeUnit.SECONDS);

        BooleanSupplier membersUpdated = () -> Arrays.stream(channels())
                .map(this::raft)
                .allMatch(r -> r.members().size() == 2);
        assertThat(eventually(membersUpdated, 10, TimeUnit.SECONDS)).isTrue();

        for (JChannel channel : channels()) {
            RAFT r = raft(channel);
            assertThat(r.members()).hasSize(2).doesNotContain(removedRaftId);
        }

        byte[] payload = new byte[] { 1 };
        newLeader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        assertCommitIndex(10_000, newLeader.lastAppended(), newLeader.lastAppended(), this::raft, channels());
    }

    public void testForceLeaderElectionThenRemoveOldLeader() throws Exception {
        withClusterSize(3);
        createCluster();
        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT oldLeader = leader();
        String oldLeaderRaftId = oldLeader.raftId();

        // Transfer leadership to a different node.
        BaseElection election = RAFT.findProtocol(BaseElection.class, raft(0), true);
        election.startForcedElection(oldLeader.getAddress())
                .toCompletableFuture().get(30, TimeUnit.SECONDS);

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT newLeader = leader();
        assertThat(newLeader.raftId()).isNotEqualTo(oldLeaderRaftId);

        // Remove the old leader from the cluster. It becomes a Learner.
        newLeader.removeServer(oldLeaderRaftId).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> oldLeader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        for (JChannel channel : channels()) {
            RAFT r = raft(channel);
            assertThat(r.members()).hasSize(2).doesNotContain(oldLeaderRaftId);
        }

        // Cluster continues to operate.
        byte[] payload = new byte[] { 1 };
        newLeader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        assertCommitIndex(10_000, newLeader.lastAppended(), newLeader.lastAppended(), this::raft, channels());
    }

    public void testEntriesSurviveLeaderSelfRemoval() throws Exception {
        withClusterSize(3);
        createCluster();
        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        String removedRaftId = leader.raftId();

        byte[] payload = new byte[] { 1 };
        for (int i = 0; i < 16; i++) {
            leader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        }
        assertCommitIndex(10_000, leader.lastAppended(), leader.lastAppended(), this::raft, channels());

        leader.removeServer(removedRaftId).get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT newLeader = leader();
        assertThat(newLeader.raftId()).isNotEqualTo(removedRaftId);

        // Entries from before the self-removal are committed on all nodes, including the former leader (now Learner).
        assertCommitIndex(10_000, newLeader.lastAppended(), newLeader.lastAppended(), this::raft, channels());

        // New leader continues to operate and the Learner catches up with the new entries.
        for (int i = 0; i < 8; i++) {
            newLeader.set(payload, 0, 1, 10, TimeUnit.SECONDS);
        }
        assertCommitIndex(10_000, newLeader.lastAppended(), newLeader.lastAppended(), this::raft, channels());
    }

    public void testConcurrentOperationsDuringLeaderSelfRemoval() throws Exception {
        withClusterSize(3);
        createCluster();
        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        String removedRaftId = leader.raftId();

        // Submit the self-removal first, its log entry gets a lower index.
        CompletableFuture<byte[]> removal = leader.removeServer(removedRaftId);

        // Submit operations while the removal is being processed.
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        byte[] payload = new byte[] { 1 };
        for (int i = 0; i < 50; i++) {
            try {
                futures.add(leader.setAsync(payload, 0, payload.length));
            } catch (Exception e) {
                break;
            }
        }

        removal.get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        // All submitted futures must complete within a bounded time.
        for (CompletableFuture<byte[]> future : futures) {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException ignored) { }
        }

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT newLeader = leader();
        assertThat(newLeader.raftId()).isNotEqualTo(removedRaftId);
    }

    public void testPendingFuturesCompleteDuringLeaderSelfRemoval() throws Exception {
        withClusterSize(3);
        createCluster();
        waitUntilAllRaftsHaveLeader(channels(), this::raft);

        RAFT leader = leader();
        String removedRaftId = leader.raftId();
        int leaderIndex = indexOf(true);

        // Block followers from receiving messages, preventing entries from committing.
        for (int i = 0; i < channels().length; i++) {
            if (i == leaderIndex) continue;
            JChannel ch = channel(i);
            ch.getProtocolStack().insertProtocol(
                    new DISCARD().discardAll(true).setAddress(ch.getAddress()),
                    ProtocolStack.Position.ABOVE, TP.class);
        }

        // Submit operations.
        // They are appended to the leader's log but cannot commit.
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        byte[] payload = new byte[] { 1 };
        for (int i = 0; i < 5; i++) {
            futures.add(leader.setAsync(payload, 0, payload.length));
        }

        // Submit the self-removal while the operations are still pending.
        CompletableFuture<byte[]> removal = leader.removeServer(removedRaftId);

        // Wait for all entries to be appended to the log.
        assertThat(eventually(() -> leader.lastAppended() >= futures.size() + 1, 10, TimeUnit.SECONDS)).isTrue();

        // Verify the operations are still pending.
        for (CompletableFuture<byte[]> future : futures) {
            assertThat(future.isDone()).isFalse();
        }

        // Resume communication, entries commit, and the leader steps down.
        for (int i = 0; i < channels().length; i++) {
            if (i == leaderIndex) continue;
            channel(i).getProtocolStack().removeProtocol(DISCARD.class);
        }

        removal.get(10, TimeUnit.SECONDS);
        assertThat(eventually(() -> leader.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();

        // All previously pending operations completed, they should not hang.
        for (CompletableFuture<byte[]> future : futures) {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException ignored) { }
        }

        waitUntilAllRaftsHaveLeader(channels(), this::raft);
        RAFT newLeader = leader();
        assertThat(newLeader.raftId()).isNotEqualTo(removedRaftId);
    }

    private int indexOf(boolean isLeader) {
        JChannel[] channels = channels();
        for (int i = 0; i < channels.length; i++) {
            if (channels[i] != null && raft(channels[i]).isLeader() == isLeader)
                return i;
        }
        throw new AssertionError((isLeader ? "Leader" : "Follower") + " not found");
    }

    private RAFT follower() {
        return raft(indexOf(false));
    }
}
