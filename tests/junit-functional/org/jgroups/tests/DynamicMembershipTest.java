package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.tests.harness.BaseRaftChannelTest;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.tests.harness.RaftAssertion;
import org.jgroups.util.Bits;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

/**
 * Tests the addServer() / removeServer) functionality
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class DynamicMembershipTest extends BaseRaftChannelTest {

    {
        // We want to change the members.
        createManually = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }


    /** Start a member not in {A,B,C} -> expects an exception */
    public void testStartOfNonMember() throws Exception {
        withClusterSize(3);
        createCluster();

        JChannel nonMember = createDisconnectedChannel("X");
        Assertions.assertThatThrownBy(() -> nonMember.connect(clusterName()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(String.format("raft-id X is not listed in members %s", getRaftMembers()));
        close(nonMember);
    }

    /** Calls addServer() on non-leader. Calling {@link org.jgroups.protocols.raft.RAFT#addServer(String)}
     * must throw an exception */
    public void testMembershipChangeOnNonLeader() throws Exception {
        withClusterSize(2);
        createCluster();
        RAFT raft=raft(1);  // non-leader B
        assertThat(raft).isNotNull();
        // Operation should fail without needing to wait on CF.
        RaftAssertion.assertLeaderlessOperationThrows(() -> raft.addServer("X"));
    }

    /** {A,B,C} +D +E -E -D. Note that we can _add_ a 6th server, as adding it requires the majority of the existing
     * servers (3 of 5). However, _removing_ the 6th server won't work, as we require the majority (4 of 6) to remove
     * the 6th server. This is because we only have 3 'real' channels (members).
     */
    public void testSimpleAddAndRemove() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels());
        Address leader=leaderAddress();
        System.out.println("leader = " + leader);
        assertThat(leader).isNotNull();
        assertSameLeader(leader, channels());
        assertMembers(5000, getRaftMembers(), 2, channels());

        List<String> new_mbrs=List.of("D", "E");
        RAFT raft=raft(leader);
        List<String> expected_mbrs=new ArrayList<>(getRaftMembers());

        // adding:
        for(String mbr: new_mbrs) {
            System.out.printf("\nAdding %s\n", mbr);
            raft.addServer(mbr);
            expected_mbrs.add(mbr);
            assertMembers(10000, expected_mbrs, expected_mbrs.size()/2+1, channels());
        }

        // removing:
        for(int i=new_mbrs.size()-1; i >= 0; i--) {
            String mbr=new_mbrs.get(i);
            System.out.printf("\nRemoving %s\n", mbr);
            raft.removeServer(mbr);
            expected_mbrs.remove(mbr);
            assertMembers(10000, expected_mbrs, expected_mbrs.size()/2+1, channels());
        }
    }

    /**
     * Tests that after adding a new member, this information persists through restarts. Since the cluster restarts,
     * it uses a persistent log instead of an in-memory.
     * <p>
     * The cluster starts with {A, B}, then C joins, and {A, B, C} restarts. Information about C is still in {A, B}.
     * </p>
     */
    public void testMembersRemainAfterRestart() throws Exception {
        // In memory log will lose the snapshot on restart.
        withClusterSize(2);
        createCluster();

        for (JChannel ch : channels()) {
            RAFT r = raft(ch);
            r.logClass(FileBasedLog.class.getName());
        }

        waitUntilAllRaftsHaveLeader(channels());
        Address leader=leaderAddress();

        System.out.println("leader = " + leader);
        assertThat(leader).isNotNull();

        assertSameLeader(leader, channels());
        assertMembers(5000, getRaftMembers(), 2, channels());
        RAFT raft=raft(leader);

        // Fill the log with some entries.
        for (int i = 0; i < 3; i++) {
            byte[] buf=new byte[Integer.BYTES];
            Bits.writeInt(i, buf, 0);
            CompletableFuture<byte[]> f=raft.setAsync(buf, 0, buf.length);
            f.get(10, TimeUnit.SECONDS);
        }

        String new_mbr="C";
        System.out.printf("Adding [%s]\n", new_mbr);

        withClusterSize(3);
        createCluster();

        // Also set 'C' to use file based log.
        raft(2).logClass(FileBasedLog.class.getName());

        raft.addServer(new_mbr).get(10, TimeUnit.SECONDS);
        assertMembers(10000, getRaftMembers(), 2, channels());

        System.out.println("\nShutdown cluster");

        JChannel[] channels = channels();
        for (int i = 0; i < channels.length; i++) {
            JChannel ch = channels[i];
            RAFT r = raft(ch);
            r.snapshot();
            Util.close(ch);
            channels[i] = null;
        }
        destroyCluster();

        System.out.println("\nRestarting cluster");

        // We restart with only 2 nodes.
        withClusterSize(2);
        createCluster();
        for (JChannel ch : channels()) {
            RAFT r = raft(ch);
            r.logClass(FileBasedLog.class.getName());
        }

        // Nodes restart using the file configuration/previous configuration.
        // Should restore member `C` from log.
        assertThat(raft(0).members())
                .as("New member should not be in initial configuration")
                .contains(new_mbr);

        List<String> extended_mbrs=new ArrayList<>(getRaftMembers());
        extended_mbrs.add(new_mbr);
        assertMembers(10_000, extended_mbrs, 2, channels());

        withClusterSize(3);
        createCluster();
        // Also set 'C' to use file based log.
        raft(2).logClass(FileBasedLog.class.getName());

        assertMembers(10_000, extended_mbrs, 2, channels());
    }

    /**
     * {A,B,C} +D +E +F +G +H +I +J
     */
    public void testAddServerSimultaneously() throws Exception {
        withClusterSize(4);
        createCluster();
        waitUntilAllRaftsHaveLeader(channels());
        Address leader = leaderAddress();
        System.out.println("leader = " + leader);
        assertThat(leader).isNotNull();
        assertSameLeader(leader, channels());
        assertMembers(5000, getRaftMembers(), getRaftMembers().size()/2+1, channels());

        final RAFT raft = raft(leader);

        final List<String> newServers = Arrays.asList("E", "F", "G");
        final CountDownLatch addServerLatch = new CountDownLatch(1);

        for (String newServer: newServers) {
            new Thread(() -> {
                try {
                    addServerLatch.await();
                    CompletableFuture<byte[]> f=raft.addServer(newServer);
                    CompletableFutures.join(f);
                    System.out.printf("[%d]: added %s successfully\n", Thread.currentThread().getId(), newServer);
                }
                catch (Throwable t) {
                    t.printStackTrace();
                }
            }).start();
        }
        addServerLatch.countDown();

        List<String> expected_mbrs=new ArrayList<>(getRaftMembers());
        expected_mbrs.addAll(newServers);
        assertMembers(20000, expected_mbrs, expected_mbrs.size()/2+1, channels());
    }

    /**
     * {A,B} -> -B -> {A} (A is the leader). Call addServer("C"): this will fail as A is not the leader anymore. Then
     * make B rejoin, and addServer("C") will succeed
     */
    public void testAddServerOnLeaderWhichCantCommit() throws Exception {
        withClusterSize(2);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels());
        Address leader=leaderAddress();
        System.out.println("leader = " + leader);
        assertThat(leader).isNotNull();

        // close non-leaders
        close(channel(1));

        RaftAssertion.assertLeaderlessOperationThrows(
                () -> raft(0).addServer("C").get(10, TimeUnit.SECONDS),
                "Adding member without leader");

        RAFT raft = raft(leader);
        assertThat(raft.isLeader()).isFalse();
        assertThat(raft(0).isLeader()).isFalse();

        // Now start B again, so that addServer("C") can succeed
        createCluster();
        waitUntilAllRaftsHaveLeader(channels());
        leader=leaderAddress();
        System.out.println("leader = " + leader);
        assertThat(leader).isNotNull();
        raft=raft(leader);

        System.out.println("-- adding member C");
        CompletableFuture<byte[]> addC = raft.addServer("C"); // adding C should now succeed, as we have a valid leader again

        // Now create and connect C
        withClusterSize(3);
        createCluster();

        addC.get(10, TimeUnit.SECONDS);
        assertMembers(10000, getRaftMembers(), 2, channels());

        // wait until everyone has committed the addServer(C) operation
        assertCommitIndex(20000, raft(leader).lastAppended(), channels());
    }

    @Test(enabled = false, description = "https://github.com/jgroups-extras/jgroups-raft/issues/245")
    public void testMaintenanceFollower() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels());

        Address leader = getRaftAddress(true);
        System.out.printf("-- leader: %s%n", leader);
        assertThat(leader).isNotNull();

        // Retrieve the leader to execute membership commands.
        RAFT raft = raft(leader);

        // Find any follower to remove.
        Address follower = getRaftAddress(false);
        RAFT followerRaft = raft(follower);

        System.out.printf("-- removing member: %s%n", followerRaft.raftId());
        // We execute the membership operation.
        raft.removeServer(followerRaft.raftId()).get(10, TimeUnit.SECONDS);

        List<String> allMembers = new ArrayList<>(getRaftMembers());
        allMembers.remove(followerRaft.raftId());
        assertMembers(10_000, allMembers, 2, actualChannels());

        System.out.printf("-- shutdown follower node %s%n", followerRaft.raftId());
        // After membership operation succeeds, we shutdown the follower.
        shutdown(findChannelIndex(follower));
        Util.waitUntilAllChannelsHaveSameView(10_000, 100, actualChannels());

        System.out.println("-- restarting node");
        // After channel is closed, we restart it.
        createCluster();

        System.out.printf("-- adding node %s again%n", followerRaft.raftId());
        // And add again the member in the Raft cluster.
        raft.addServer(followerRaft.raftId()).get(10, TimeUnit.SECONDS);
        assertMembers(10_000, getRaftMembers(), 2, actualChannels());
    }

    public void testMaintenanceLeader() throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels());

        Address leader = getRaftAddress(true);
        System.out.printf("-- current leader: %s%n", leader);
        RAFT leaderRaft = raft(leader);

        assertMembers(5_000, getRaftMembers(), 2, actualChannels());

        // Populate with some operations.
        for (int i = 0; i < 10; i++) {
            leaderRaft.set(new byte[] { (byte) i}, 0, 1, 5, TimeUnit.SECONDS);
        }

        System.out.printf("-- removing leader %s from members%n", leaderRaft.raftId());
        leaderRaft.removeServer(leaderRaft.raftId()).get(10, TimeUnit.SECONDS);

        // After the operation finishes, the leader has already stepped down.
        assertThat(leaderRaft.isLeader()).isFalse();


        // A majority eventually commits the operation, this guarantees the log-prefix.
        long afterRemovalIndex = leaderRaft.commitIndex();
        assertThat(Arrays.stream(channels())
                .map(this::raft)
                .filter(r -> r.lastAppended() == afterRemovalIndex)).hasSizeGreaterThanOrEqualTo(2);
        BooleanSupplier bs = () -> Arrays.stream(channels())
                .map(this::raft)
                .filter(r -> r.commitIndex() == afterRemovalIndex).count() >= 2;
        assertThat(eventually(bs, 10, TimeUnit.SECONDS))
                .withFailMessage(() -> "The majority did not commit the leader removal: \n" + Arrays.stream(channels()).map(c -> raft(c).toString()).collect(Collectors.joining(System.lineSeparator())))
                .isTrue();

        List<String> allMembers = new ArrayList<>(getRaftMembers());
        allMembers.remove(leaderRaft.raftId());

        assertMembers(10_000, allMembers, 2, actualChannels());

        System.out.printf("-- old leader %s does not apply operations%n", leaderRaft.raftId());
        RaftAssertion.assertLeaderlessOperationThrows(() -> leaderRaft.set(new byte[1], 0, 1, 5, TimeUnit.SECONDS));

        System.out.printf("-- waiting old leader %s to step down%n", leaderRaft.raftId());
        RAFT[] rafts = Arrays.stream(channels())
                .map(this::raft)
                .filter(r -> !r.raftId().equals(leaderRaft.raftId()))
                .toArray(RAFT[]::new);
        BaseRaftElectionTest.waitUntilLeaderElected(rafts, 10_000);

        System.out.println("-- shutdown old leader server");
        shutdown(findChannelIndex(leader));
        // FIXME: remove next line after https://github.com/jgroups-extras/jgroups-raft/issues/245
        RaftTestUtils.deleteRaftLog(leaderRaft);
        Util.waitUntilAllChannelsHaveSameView(10_000, 100, actualChannels());

        String raftId = leaderRaft.raftId();
        System.out.printf("-- recreating previous leader %s%n", raftId);
        createCluster();

        leader = getRaftAddress(true);
        System.out.printf("-- new leader is %s%n", leader);
        assertThat(leader).isNotNull();
        RAFT newLeaderRaft = raft(leader);

        newLeaderRaft.addServer(raftId).get(10, TimeUnit.SECONDS);
        assertMembers(10_000, getRaftMembers(), 2, channels());
    }

    public void testLeaderLeavingChangesMajority() throws Exception {
        withClusterSize(2);
        createCluster();

        waitUntilAllRaftsHaveLeader(channels());

        Address leader = getRaftAddress(true);
        System.out.printf("-- current leader: %s%n", leader);
        RAFT leaderRaft = raft(leader);

        int leaderIndex = findChannelIndex(leader);
        int followerIndex = findChannelIndex(getRaftAddress(false));
        System.out.printf("-- leader is %d and follower %d%n", leaderIndex, followerIndex);

        assertMembers(5_000, getRaftMembers(), 2, actualChannels());

        System.out.printf("-- removing leader %s from members%n", leaderRaft.raftId());
        leaderRaft.removeServer(leaderRaft.raftId()).get(10, TimeUnit.SECONDS);

        // A majority eventually commits the operation, this guarantees the log-prefix.
        long afterRemovalIndex = leaderRaft.commitIndex();
        BooleanSupplier bs = () -> Arrays.stream(channels())
                .map(this::raft)
                .filter(r -> r.commitIndex() == afterRemovalIndex).count() >= 2;
        assertThat(eventually(bs, 10, TimeUnit.SECONDS))
                .withFailMessage(() -> "The majority did not commit the leader removal: \n" + Arrays.stream(channels()).map(c -> raft(c).toString()).collect(Collectors.joining(System.lineSeparator())))
                .isTrue();

        List<String> allMembers = new ArrayList<>(getRaftMembers());
        allMembers.remove(leaderRaft.raftId());

        assertMembers(10_000, allMembers, 1, channels());

        System.out.printf("-- old leader %s does not apply operations%n", leaderRaft.raftId());
        RaftAssertion.assertLeaderlessOperationThrows(() -> leaderRaft.set(new byte[1], 0, 1, 5, TimeUnit.SECONDS));

        System.out.printf("-- waiting old leader %s to step down%n", leaderRaft.raftId());
        assertThat(eventually(() -> raft(followerIndex).isLeader(), 10, TimeUnit.SECONDS))
                .withFailMessage(() -> "Leader was: " + raft(1).leader())
                .isTrue();

        System.out.println("-- shutdown old leader server");
        close(leaderIndex);
        Util.waitUntilAllChannelsHaveSameView(10_000, 100, actualChannels());

        System.out.println("-- still leader after old leader leaves");
        assertThat(raft(followerIndex).isLeader()).isTrue();
        assertThat(raft(followerIndex).members())
                .hasSize(1)
                .containsOnly(raft(followerIndex).raftId());
    }

    private int findChannelIndex(Address address) {
        for (int i = 0; i < actualChannels().length; i++) {
            if (channel(i).getAddress().equals(address))
                return i;
        }

        throw new IllegalStateException("Channel not found");
    }

    protected void assertSameLeader(Address leader, JChannel... channels) {
        for(JChannel ch: channels) {
            final Address raftLeader = raft(ch).leader();
            assertThat(raftLeader)
                    .as(() -> String.format("expected leader to be '%s' but was '%s'", leader, raftLeader))
                    .satisfiesAnyOf(l -> assertThat(l).isNull(), l -> assertThat(l).isEqualTo(leader));
        }
    }

    protected void assertMembers(long timeout, Collection<String> members, int expected_majority, JChannel... channels) {
        BooleanSupplier bs = () -> {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                if(!ch.isConnected())
                    continue;
                RAFT raft=raft(ch);
                if(!new HashSet<>(raft.members()).equals(new HashSet<>(members)))
                    all_ok=false;
            }
            return all_ok;
        };
        Supplier<String> message = () -> Arrays.stream(channels)
                .map(ch -> {
                    if (!ch.isConnected()) {
                        return String.format("%s -- disconnected", ch.getName());
                    }
                    RAFT r = raft(ch);
                    return String.format("%s: %s", r.raftId(), r.members());
                })
                .collect(Collectors.joining(System.lineSeparator())) + " while waiting for " + members;

        assertThat(eventually(bs, timeout, TimeUnit.MILLISECONDS))
                .as(message)
                .isTrue();

        for(JChannel ch: channels) {
            if(!ch.isConnected())
                continue;

            RAFT raft=raft(ch);
            System.out.printf("%s: members=%s, majority=%d\n", ch.getAddress(), raft.members(), raft.majority());

            assertThat(Set.of(raft.members()))
                    .as(() -> String.format("expected members=%s, actual members=%s", members, raft.members()))
                    .containsExactlyInAnyOrderElementsOf(Set.of(raft.members()));

            assertThat(raft.majority())
                    .as(() -> ch.getName() + ": expected majority=" + expected_majority + ", actual=" + raft.majority())
                    .isEqualTo(expected_majority);
        }
    }

    private Address getRaftAddress(boolean leader) {
        for (JChannel ch : actualChannels()) {
            RAFT r = raft(ch);
            if (leader == r.isLeader())
                return r.getAddress();
        }

        throw new IllegalStateException("Did not found node with leader? " + leader);
    }

    protected void assertCommitIndex(long timeout, long expected_commit, JChannel... channels) {
        BooleanSupplier bs = () -> {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                RAFT raft=raft(ch);
                if(expected_commit != raft.commitIndex())
                    all_ok=false;
            }
            return all_ok;
        };

        assertThat(eventually(bs, timeout, TimeUnit.MILLISECONDS))
                .as("Commit index never match between channels")
                .isTrue();

        for(JChannel ch: channels) {
            RAFT raft=raft(ch);
            System.out.printf("%s: members=%s, last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.members(),
                              raft.lastAppended(), raft.commitIndex());

            assertThat(raft.commitIndex())
                    .as(() -> String.format("%s: last-applied=%d, commit-index=%d", ch.getAddress(), raft.lastAppended(), raft.commitIndex()))
                    .isEqualTo(expected_commit);
        }
    }

    protected void waitUntilAllRaftsHaveLeader(JChannel[] channels) throws TimeoutException {
        RAFT[] rafts = Arrays.stream(channels)
                .filter(Objects::nonNull)
                .map(this::raft)
                .toArray(RAFT[]::new);
        BaseRaftElectionTest.waitUntilLeaderElected(rafts, 10_000);
    }

    protected RAFT raft(Address addr) {
        return raft(channel(addr));
    }
}
