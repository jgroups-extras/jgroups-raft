package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests that a member cannot vote twice. Issue: https://github.com/belaban/jgroups-raft/issues/24
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class VoteTest {
    protected JChannel[]                channels;
    protected RAFT[]                    rafts;
    protected Address                   leader;
    protected static final String       CLUSTER=VoteTest.class.getSimpleName();
    protected static final List<String> mbrs=Arrays.asList("A", "B", "C", "D");

    @AfterMethod protected void destroy() {
        close(true, true, channels);
    }


    /** Start a member not in {A,B,C} -> expects an exception */
    public void testStartOfNonMember() {
        JChannel non_member=null;
        try {
            init("X");
            assert false : "Starting a non-member should throw an exception";
        }
        catch(Exception e) {
            System.out.println("received exception as expected: " + e.toString());
        }
        finally {
            close(true, true, non_member);
        }
    }


    /**
     * Membership is {A,B,C,D}, majority 3. Members A and B are up. Try to append an entry won't work as A and B don't
     * have the majority. Now restart B. The entry must still not be able to commit as B's vote shouldn't count twice.<p/>
     * https://github.com/belaban/jgroups-raft/issues/24
     */
    public void testMemberVotesTwice() throws Exception {
        init("A", "B", "C");
        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        assertSameLeader(leader, channels);
        RAFT raft=raft(leader);

        // kill a non-leader, so we fall below the majority and cannot commit
        int index=nonLeader(channels);
        JChannel non_leader=channels[index];
        String name=non_leader.getName();
        System.out.println("---> Stopping " + name);
        Util.close(non_leader);

        try {
            raft.set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS);
            assert false : "the change should have failed as we don't have a majority of 3 to commit it";
        }
        catch(TimeoutException ex) {
            System.out.println("Caught an exception as expected, trying to commit a change: " + ex);
        }

        // the leader needs to have a last_applied of 1 and everybody (including the leader) needs to have a commit_index of 0
        for(JChannel ch: channels) {
            if(!ch.isConnected())
                continue;
            RAFT r=raft(ch);
            System.out.println(ch.getAddress() + ": last_applied=" + r.lastApplied() + ", commit_index=" + r.commitIndex());
            assert r.commitIndex() == 0 : "commit_index of " + ch.getName() + " should be 0 (was " + r.commitIndex() + ")";
            int actual_last_applied=r.lastApplied();
            assert actual_last_applied == 1 : "expected last_applied=" + 1 + ", but got " + actual_last_applied;
        }


        // Now kill another non-member and restart it
        index=nonLeader(channels);
        non_leader=channels[index];
        name=non_leader.getName();
        System.out.println("---> Stopping " + name);
        Util.close(non_leader);

        // Now start the previously killed member again
        System.out.println("--> Starting " + name);
        channels[index]=create(name);

        // commit_index should still be 0 !
        Util.sleep(2000);
        for(JChannel ch: channels) {
            if(!ch.isConnected())
                continue;
            RAFT r=raft(ch);
            System.out.println(ch.getAddress() + ": last_applied=" + r.lastApplied() + ", commit_index=" + r.commitIndex());
        }
        for(JChannel ch: channels) {
            if(!ch.isConnected())
                continue;
            RAFT r=raft(ch);
            System.out.println(ch.getAddress() + ": last_applied=" + r.lastApplied() + ", commit_index=" + r.commitIndex());
            assert r.commitIndex() == 0 : "commit_index of " + ch.getName() + " should be 0 (was " + r.commitIndex() + ")";
            int actual_last_applied=r.lastApplied();
            assert actual_last_applied == 1 : "expected last_applied=" + 1 + ", but got " + actual_last_applied;
        }
    }



    protected void init(String ... nodes) throws Exception {
        channels=new JChannel[nodes.length];
        rafts=new RAFT[nodes.length];
        for(int i=0; i < nodes.length; i++) {
            channels[i]=create(nodes[i]);
            rafts[i]=raft(channels[i]);
        }
    }

    protected JChannel create(String name) throws Exception {
        RAFT raft=new RAFT().members(mbrs).raftId(name).stateMachine(new DummyStateMachine())
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logName(name + "-" + CLUSTER);
        JChannel ch=new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(name);
        ch.connect(CLUSTER);
        return ch;
    }

    protected static Address leader(long timeout, long interval, JChannel ... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            for(JChannel ch : channels) {
                if(ch.isConnected() && raft(ch).leader() != null)
                    return raft(ch).leader();
            }
            Util.sleep(interval);
        }
        return null;
    }

    protected static int nonLeader(JChannel ... channels) {
        for(int i=channels.length-1; i >= 0; i--) {
            JChannel ch=channels[i];
            if(!ch.isConnected())
                continue;
            if(!raft(ch).leader().equals(ch.getAddress()))
                return i;
        }
        return -1;
    }

    protected void assertSameLeader(Address leader, JChannel ... channels) {
        for(JChannel ch: channels)
            assert leader.equals(raft(ch).leader());
    }



    protected void assertCommitIndex(long timeout, long interval, int expected_commit, JChannel... channels) {
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
                              raft.lastApplied(), raft.commitIndex());
            assert raft.commitIndex() == expected_commit : String.format("%s: last-applied=%d, commit-index=%d",
                                                                         ch.getAddress(), raft.lastApplied(), raft.commitIndex());
        }
    }

    protected RAFT raft(Address addr) {
        return raft(channel(addr));
    }

    protected JChannel channel(Address addr) {
        for(JChannel ch: Arrays.asList(channels)) {
            if(ch.getAddress() != null && ch.getAddress().equals(addr))
                return ch;
        }
        return null;
    }

    protected static RAFT raft(JChannel ch) {
        return (RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
    }

    protected void close(boolean remove_log, boolean remove_snapshot, JChannel ... channels) {
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            RAFT raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
            if(remove_log)
                raft.log().delete(); // remove log files after the run
            if(remove_snapshot)
                raft.deleteSnapshot();
            Util.close(ch);
        }
    }

    protected static class DummyStateMachine implements StateMachine {
        public byte[] apply(byte[] data, int offset, int length) throws Exception {return new byte[0];}
        public void readContentFrom(DataInput in) throws Exception {}
        public void writeContentTo(DataOutput out) throws Exception {}
    }
}
