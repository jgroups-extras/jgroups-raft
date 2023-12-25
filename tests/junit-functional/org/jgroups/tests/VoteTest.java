package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.tests.harness.RaftAssertion;
import org.jgroups.util.Util;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

/**
 * Tests that a member cannot vote twice. Issue: https://github.com/belaban/jgroups-raft/issues/24
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class VoteTest extends BaseRaftElectionTest.ChannelBased {

    {
        createManually = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }


    /** Start a member not in {A,B,C} -> expects an exception */
    public void testStartOfNonMember(Class<?> ignore) throws Exception {
        withClusterSize(2);
        JChannel non_member=createDisconnectedChannel("X");
        Assertions.assertThatThrownBy(() -> non_member.connect(clusterName()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("raft-id X is not listed in members");
        close(non_member);
    }


    /**
     * Membership is {A,B,C,D}, majority 3. Members A and B are up. Try to append an entry won't work as A and B don't
     * have the majority. Now restart B. The entry must still not be able to commit as B's vote shouldn't count twice.<p/>
     * https://github.com/belaban/jgroups-raft/issues/24
     * @deprecated Voting has changed in 1.0.6, so this test is moot (https://github.com/belaban/jgroups-raft/issues/20)
     */
    @Deprecated
    public void testMemberVotesTwice(Class<?> ignore) throws Exception {
        withClusterSize(4);
        createCluster();

        // close C and D
        close(2);
        close(3);

        // A and B: {A,B}
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channel(0), channel(1));
        RaftAssertion.assertLeaderlessOperationThrows(() -> raft(0).set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS));

        // close B and create a new B'
        System.out.printf("restarting %s\n", channel(1).name());
        close(1);
        createCluster(1);

        // Try the change again: we have votes from A and B from before the non-leader was restarted. Now B was
        // restarted, but it cannot vote again in the same term, so we still only have 2 votes!
        RaftAssertion.assertLeaderlessOperationThrows(() -> raft(0).set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS));

        // now start C. as we have a majority now (A,B,C), the change should succeed
        System.out.println("starting C");
        createCluster(1);

        // wait until we have a leader (this may take a few ms)
        waitUntilLeaderElected(10_000, 0, 1, 2);

        // need to set this again, as the leader might have changed
        RAFT raft=leader();
        assertThat(raft).isNotNull();

        // This time, we should succeed
        raft.set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS);

        BooleanSupplier bs = () -> Stream.of(actualChannels())
                .filter(JChannel::isConnected)
                .map(this::raft)
                .allMatch(r -> r.commitIndex() == 1 && r.lastAppended() == 1);
        Supplier<String> message = () -> Stream.of(actualChannels())
                .map(this::raft)
                .map(r -> String.format("%s: append-index=%d, commit-index=%d\n", r.getAddress(), r.lastAppended(), r.commitIndex()))
                .collect(Collectors.joining(System.lineSeparator()));

        assertThat(eventually(bs, 10, TimeUnit.SECONDS)).as(message).isTrue();
    }

    /** Membership=A, member=A: should become leader immediately */
    public void testSingleMember(Class<?> ignore) throws Exception {
        withClusterSize(1);
        createCluster();

        waitUntilLeaderElected(10_000, 0);
        Address leader=leaderAddress();
        System.out.println("leader = " + leader);
        assertThat(leader).isNotNull();
        assertThat(leader).isEqualTo(channel(0).getAddress());
    }

    /** {A,B,C} with leader A. Then B and C leave: A needs to become Follower */
    public void testLeaderGoingBackToFollower(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster();

        waitUntilLeaderElected(5_000, 0, 1, 2);
        RAFT raft=leader();
        assertThat(raft).isNotNull();

        System.out.printf("leader is %s\n", raft.raftId());
        System.out.println("closing non-leaders:");

        JChannel[] channels = channels();
        for (int i = 0; i < channels.length; i++) {
            JChannel ch = channels[i];
            if(ch.getAddress().equals(raft.getAddress()))
                continue;
            close(i);
        }

        Util.waitUntil(5000, 500, () -> !raft.isLeader());
        assertThat(raft.leader()).isNull();
    }


    /** {A,B,C,D}: A is leader and A, B, C and D have leader=A. When C and D are closed, both A, B and C must
     * have leader set to null, as there is no majority (3) any longer */
    public void testNullLeader(Class<?> ignore) throws Exception {
        withClusterSize(4);
        createCluster();

        // assert we have a leader
        waitUntilLeaderElected(10_000, 0, 1, 2, 3);

        // close C and D, now everybody should have a null leader
        close(3);
        close(2);

        Util.waitUntilAllChannelsHaveSameView(10_000, 250, channel(0), channel(1));
        BooleanSupplier bs = () -> Stream.of(actualChannels())
                .filter(JChannel::isConnected)
                .map(this::raft)
                .allMatch((RAFT r) -> r.leader() == null);
        assertThat(eventually(bs, 10, TimeUnit.SECONDS)).as(this::printLeaders).isTrue();
        System.out.printf("channels:\n%s", printLeaders());
    }

    protected String printLeaders() {
        StringBuilder sb=new StringBuilder("\n");
        for(JChannel ch: actualChannels()) {
            if(!ch.isConnected())
                sb.append(String.format("%s: not connected\n", ch.getName()));
            else {
                RAFT raft=raft(ch);
                sb.append(String.format("%s: leader=%s\n", ch.getName(), raft.leader()));
            }
        }
        return sb.toString();
    }

    protected RAFT raft(Address addr) {
        return raft(channel(addr));
    }

}
