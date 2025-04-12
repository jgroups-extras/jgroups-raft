package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.tests.harness.BaseRaftChannelTest;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.tests.harness.RaftAssertion;
import org.jgroups.util.Bits;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

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

import static org.assertj.core.api.Assertions.assertThat;

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

        assertThat(RaftTestUtils.eventually(bs, timeout, TimeUnit.MILLISECONDS))
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

        assertThat(RaftTestUtils.eventually(bs, timeout, TimeUnit.MILLISECONDS))
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
