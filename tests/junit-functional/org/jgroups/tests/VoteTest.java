package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Tests that a member cannot vote twice. Issue: https://github.com/belaban/jgroups-raft/issues/24
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class VoteTest {
    protected JChannel[]                channels;
    protected RAFT[]                    rafts;
    protected static final String       CLUSTER=VoteTest.class.getSimpleName();

    @AfterMethod protected void destroy() {
        if(channels != null)
            close(true, true, channels);
    }


    /** Start a member not in {A,B,C} -> expects an exception */
    public void testStartOfNonMember() throws Exception {
        JChannel non_member=null;
        try {
            non_member=create("X", Arrays.asList("A", "B"));
            assert false : "Starting a non-member should throw an exception";
        }
        catch(Exception e) {
            System.out.println("received exception as expected: " + e);
        }
        finally {
            close(true, true, non_member);
        }
    }


    /**
     * Membership is {A,B,C,D}, majority 3. Members A and B are up. Try to append an entry won't work as A and B don't
     * have the majority. Now restart B. The entry must still not be able to commit as B's vote shouldn't count twice.<p/>
     * https://github.com/belaban/jgroups-raft/issues/24
     * @deprecated Voting has changed in 1.0.6, so this test is moot (https://github.com/belaban/jgroups-raft/issues/20)
     */
    @Deprecated
    public void testMemberVotesTwice() throws Exception {
        init("A", "B", "C", "D");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        Util.close(channels[2], channels[3]); // close C and D
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels[0], channels[1]); // A and B: {A,B}

        RAFT raft=channels[0].getProtocolStack().findProtocol(RAFT.class);

        try {
            raft.set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS);
            assert false : "the change should have failed as we don't have a majority of 3 to commit it";
        }
        catch(IllegalStateException ex) {
            System.out.println("Caught an exception as expected, trying to commit a change: " + ex);
        }

        // close B and create a new B'
        System.out.printf("restarting %s\n", channels[1].name());
        Util.close(channels[1]);

        channels[1]=create("B", Arrays.asList("A", "B", "C", "D"));
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels[0], channels[1]);

        // Try the change again: we have votes from A and B from before the non-leader was restarted. Now B was
        // restarted, but it cannot vote again in the same term, so we still only have 2 votes!
        try {
            raft.set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS);
            assert false : "the change should have failed as we don't have a majority of 3 to commit it";
        }
        catch(IllegalStateException ex) {
            System.out.println("Caught an exception as expected, trying to commit a change: " + ex);
        }

        // now start C. as we have a majority now (A,B,C), the change should succeed
        System.out.println("starting C");
        channels[2]=create("C", Arrays.asList("A", "B", "C", "D"));
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels[0], channels[1], channels[2]);

        // wait until we have a leader (this may take a few ms)
        Util.waitUntil(10000, 500,
                       () -> Stream.of(channels).filter(JChannel::isConnected)
                         .anyMatch(c -> ((RAFT)c.getProtocolStack().findProtocol(RAFT.class)).isLeader()));

        // need to set this again, as the leader might have changed
        raft=null;
        for(JChannel c: channels) {
            if(!c.isConnected())
                continue;
            RAFT r=c.getProtocolStack().findProtocol(RAFT.class);
            if(r.isLeader()) {
                raft=r;
                break;
            }
        }
        assert raft != null;

        // This time, we should succeed
        raft.set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS);

        Util.waitUntil(10000, 500, () -> Stream.of(channels).filter(JChannel::isConnected)
          .map(c -> (RAFT)c.getProtocolStack().findProtocol(RAFT.class))
          .allMatch(r -> r.commitIndex() == 1 && r.lastAppended() == 1));
        for(JChannel ch: channels) {
            if(!ch.isConnected())
                continue;
            RAFT r=ch.getProtocolStack().findProtocol(RAFT.class);
            System.out.printf("%s: append-index=%d, commit-index=%d\n", ch.getAddress(), r.lastAppended(), r.commitIndex());
        }
    }

    /** Membership=A, member=A: should become leader immediately */
    public void testSingleMember() throws Exception {
        channels=new JChannel[]{create("A", Collections.singletonList("A"))};
        rafts=new RAFT[]{raft(channels[0])};
        Address leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        assert leader.equals(channels[0].getAddress());
    }

    /** {A,B,C} with leader A. Then B and C leave: A needs to become Follower */
    public void testLeaderGoingBacktoFollower() throws Exception {
        init("A", "B", "C");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        JChannel leader_ch=getLeader(10000, 500, channels);
        RAFT raft=raft(leader_ch);
        System.out.printf("leader is %s\n", leader_ch);
        System.out.println("closing non-leaders:");
        // Stream.of(channels).filter(c -> !c.getAddress().equals(leader_ch.getAddress())).forEach(JChannel::close);

        for(JChannel ch: channels) {
            if(ch.getAddress().equals(leader_ch.getAddress()))
                continue;
            Util.close(ch);
        }

        Util.waitUntil(5000, 500, () -> !raft.isLeader());
        assert raft.leader() == null;
    }


    /** {A,B,C,D}: A is leader and A, B, C and D have leader=A. When C and D are closed, both A, B and C must
     * have leader set to null, as there is no majority (3) any longer */
    public void testNullLeader() throws Exception {
        init("A", "B", "C", "D");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        // assert we have a leader
        Util.waitUntil(10000, 500,
                       () -> Stream.of(channels)
                         .map(c -> (RAFT)c.getProtocolStack().findProtocol(RAFT.class))
                         .allMatch((RAFT r) -> r.leader() != null));
        Util.close(channels[2], channels[3]); // close C and D, now everybody should have a null leader
        Util.waitUntil(10000, 500,
                       () -> Stream.of(channels).filter(JChannel::isConnected)
                         .map(VoteTest::raft)
                         .allMatch((RAFT r) -> r.leader() == null),
                       this::printLeaders);
        System.out.printf("channels:\n%s", printLeaders());
    }


    protected void init(String ... nodes) throws Exception {
        channels=new JChannel[nodes.length];
        rafts=new RAFT[nodes.length];
        for(int i=0; i < nodes.length; i++) {
            channels[i]=create(nodes[i], Arrays.asList(nodes));
            rafts[i]=raft(channels[i]);
        }
    }

    protected String printLeaders() {
        StringBuilder sb=new StringBuilder("\n");
        for(JChannel ch: channels) {
            if(!ch.isConnected())
                sb.append(String.format("%s: not connected\n", ch.getName()));
            else {
                RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
                sb.append(String.format("%s: leader=%s\n", ch.getName(), raft.leader()));
            }
        }
        return sb.toString();
    }


    protected static JChannel create(String name, List<String> mbrs) throws Exception {
        RAFT raft=new RAFT().members(mbrs).raftId(name).stateMachine(new DummyStateMachine())
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(name + "-" + CLUSTER);
        JChannel ch=new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(name);
        ch.connect(CLUSTER);
        return ch;
    }


    protected static Address leader(long timeout, long interval, JChannel ... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            for(JChannel ch : channels) {
                if(ch.isConnected() && raft(ch).isLeader())
                    return raft(ch).leader();
            }
            Util.sleep(interval);
        }
        return null;
    }

    protected static JChannel getLeader(long timeout, long interval, JChannel ... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            for(JChannel ch : channels) {
                if(ch.isConnected() && raft(ch).isLeader())
                    return ch;
            }
            Util.sleep(interval);
        }
        return null;
    }


    protected static JChannel nonLeader(JChannel ... channels) {
        return Stream.of(channels).filter(c -> c.isConnected() && !raft(c).leader().equals(c.getAddress()))
          .filter(c -> c.getProtocolStack().findProtocol(DISCARD.class) == null)
          .findFirst().orElse(null);
    }

    protected static void discardAll(JChannel ... channels) throws Exception {
        for(JChannel ch: channels)
            ch.getProtocolStack().insertProtocol(new DISCARD().discardAll(true), ProtocolStack.Position.ABOVE, TP.class);
    }

    protected static void assertSameLeader(Address leader, JChannel... channels) {
        for(JChannel ch: channels)
            assert leader.equals(raft(ch).leader());
    }



    protected static void assertCommitIndex(long timeout, long interval, int expected_commit, JChannel... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                RAFT raft=raft(ch);
                if(expected_commit != raft.commitIndex())
                    all_ok=false;
            }
            if(all_ok)
                break;
            Util.sleep(interval);
        }
        for(JChannel ch: channels) {
            RAFT raft=raft(ch);
            System.out.printf("%s: members=%s, last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.members(),
                              raft.lastAppended(), raft.commitIndex());
            assert raft.commitIndex() == expected_commit : String.format("%s: last-applied=%d, commit-index=%d",
                                                                         ch.getAddress(), raft.lastAppended(), raft.commitIndex());
        }
    }

    protected RAFT raft(Address addr) {
        return raft(channel(addr));
    }

    protected JChannel channel(Address addr) {
        for(JChannel ch: channels) {
            if(ch.getAddress() != null && ch.getAddress().equals(addr))
                return ch;
        }
        return null;
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }

    protected static void close(boolean remove_log, boolean remove_snapshot, JChannel... channels) {
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            ProtocolStack stack=ch.getProtocolStack();
            stack.removeProtocol(DISCARD.class);
            RAFT raft=stack.findProtocol(RAFT.class);
            if(remove_log) {
                try {
                    raft.log().delete(); // remove log files after the run
                }
                catch(Exception ignored) {}
            }
            if(remove_snapshot)
                raft.deleteSnapshot();
            Util.close(ch);
        }
    }

}
