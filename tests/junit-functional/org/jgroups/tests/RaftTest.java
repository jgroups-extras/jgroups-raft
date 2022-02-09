package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.raft.*;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.SampleStateMachine;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the various stages of the Raft protocoll, e.g. regular append, incorrect append, snapshots, leader change,
 * leader election etc
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RaftTest {
    protected JChannel            a, b, c;
    protected RaftHandle          rha, rhb;
    protected RAFT                raft_a, raft_b, raft_c;
    protected final SampleStateMachine  sma=new SampleStateMachine(),
      smb=new SampleStateMachine(), smc=new SampleStateMachine();
    protected static final String GRP="RaftTest";

    @BeforeMethod
    protected void create() throws Exception {
        a=create("A", 600_000, 1_000_000);
        rha=new RaftHandle(a, sma);
        a.connect(GRP);
        raft_a=raft(a).leader(a.getAddress()).changeRole(Role.Leader);

        b=create("B", 600_000, 1_000_000);
        rhb=new RaftHandle(b, smb);
        b.connect(GRP);
        raft_b=raft(b).leader(a.getAddress()).changeRole(Role.Follower);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
        assert raft_a.isLeader();
        assert !raft_b.isLeader();
    }

    @AfterMethod protected void destroy() throws Exception {
        for(JChannel ch: Arrays.asList(c, b, a)) {
            if(ch == null)
                continue;
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            raft.log().delete();
            Util.close(ch);
        }
    }



    public void testRegularAppend() throws Exception {
        int prev_value=add(rha, 1);
        assert prev_value == 0;
        assert sma.counter() == 1;
        assert smb.counter() == 0; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(1, 1, 0, raft_a);
        assertIndices(1, 0, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 0, 1, 2);

        prev_value=add(rha, 2);
        assert prev_value == 1;
        assert sma.counter() == 3;
        assert smb.counter() == 1; // previous value; B is always lagging one commit behind
        assertCommitTableIndeces(b.getAddress(), raft_a, 1, 2, 3);

        prev_value=add(rha, 3);
        assert prev_value == 3;
        assert sma.counter() == 6;
        assert smb.counter() == 3; // previous value; B is always lagging one commit behind
        assertIndices(3, 3, 0, raft_a);
        assertIndices(3, 2, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 2, 3, 4);

        prev_value=add(rha, -3);
        assert prev_value == 6;
        assert sma.counter() == 3;
        assert smb.counter() == 6; // previous value; B is always lagging one commit behind
        assertIndices(4, 4, 0, raft_a);
        assertIndices(4, 3, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 3, 4, 5);

        for(int i=1,prev=3; i <= 1000; i++) {
            prev_value=add(rha, 5);
            assert prev_value == prev;
            prev+=5;
        }
        assert sma.counter() == 5000 + 3;
        assert smb.counter() == sma.counter() - 5;

        assertIndices(1004, 1004, 0, raft_a);
        assertIndices(1004, 1003, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 1003, 1004, 1005);

        int current_term=raft_a.currentTerm(), expected_term;
        raft_a.currentTerm(expected_term=current_term + 10);

        for(int i=1; i <= 7; i++)
            add(rha, 1);

        assert sma.counter() == 5010;
        assert smb.counter() == sma.counter() - 1;

        assertIndices(1011, 1011, expected_term, raft_a);
        assertIndices(1011, 1010, expected_term, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 1010, 1011, 1012);
    }


    public void testAppendSameElementTwice() throws Exception {
        raft_a.currentTerm(20);
        for(int i=1; i <= 5; i++)
            add(rha, 1);
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 4, 20, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 4, 5, 6);
        assert sma.counter() == 5;
        assert smb.counter() == 4;

        // this will append the same entry, but because index 4 is < last_appended (5), it will not get appended
        // Note though that the commit-index will be updated
        sendAppendEntriesRequest(raft_a, 3, 20, 20, 5);
        Util.waitUntil(5000, 50, () -> raft_b.commitIndex() == 5);
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 5, 20, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 5, 5, 6);
        assert sma.counter() == 5;
        assert smb.counter() == 5;
    }

    public void testAppendBeyondLast() throws Exception {
        raft_a.currentTerm(22);
        for(int i=1; i <= 5; i++)
            add(rha, 1);
        assert sma.counter() == 5;
        assert smb.counter() == 4; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 4, 22, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 4, 5, 6);

        // now append beyond the end
        sendAppendEntriesRequest(raft_a, 9, 22, 22, 0);
        Util.sleep(1000);
        // nothing changed, as request was rejected
        assertCommitTableIndeces(b.getAddress(), raft_a, 4, 5, 6);
    }

    /** Tests appding with correct prev_index, but incorrect term */
    public void testAppendWrongTerm() throws Exception {
        raft_a.currentTerm(22);
        for(int i=1; i <= 15; i++) {
            if(i % 5 == 0)
                raft_a.currentTerm(raft_a.currentTerm()+1);
            add(rha, 1);
        }
        assert sma.counter() == 15;
        assert smb.counter() == 14; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(15, 15, 25, raft_a);
        assertIndices(15, 14, 25, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 14, 15, 16);

        // now append beyond the end
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        // send a correct index, but incorrect prev_term:
        int incorrect_prev_term=24;
        int commit_index=raft_a.commitIndex(), prev_index=14;

        sendAppendEntriesRequest(raft_a, prev_index, incorrect_prev_term, 30, commit_index);
        Util.sleep(2000);
        assertCommitTableIndeces(b.getAddress(), raft_a, 4, 5, 6);
    }



    /** Tests appends where we change prev_term, so that we'll get an AppendResult with success=false */
    public void testIncorrectAppend() throws Exception {
        int prev_value=add(rha, 1);
        assert prev_value == 0;
        assert sma.counter() == 1;
        assert smb.counter() == 0; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(1, 1, 0, raft_a);
        assertIndices(1, 0, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 0, 1, 2);

        raft_a.currentTerm(2);
        prev_value=add(rha, 1);
        assert prev_value == 1;
        prev_value=add(rha, 1);
        assert prev_value == 2;
        assert sma.counter() == 3;
        assert smb.counter() == 2; // previous value; B is always lagging one commit behind
        assertCommitTableIndeces(b.getAddress(), raft_a, 2, 3, 4);

        raft_a.currentTerm(4);
        for(int i=1; i <= 3; i++)
            add(rha, 1);

        int last_correct_append_index=raft_a.lastAppended(); // 6
        int index=last_correct_append_index+1;

        // add some incorrect entries with term=5 to B:
        RaftImpl impl=raft_b.impl();
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        // 7
        AppendResult result=impl.handleAppendEntriesRequest(val, 0, val.length, a.getAddress(), index - 1, 4, 5, 1, false);
        assert result.success();
        raft_b.currentTerm(5);
        index++;

        // 8
        result=impl.handleAppendEntriesRequest(val, 0, val.length, a.getAddress(), index-1, 5, 5, 1, false);
        assert result.success();
        assertIndices(8, 5, 5, raft_b);

        raft_a.currentTerm(7);
        for(int i=1; i <= 2; i++)
            add(rha, -1);

        // last_appended indices 7 and 8 need to be replaced with term=7
        assert sma.counter() == 4;
        assert smb.counter() == 5; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(8, 8, 7, raft_a);
        assertIndices(8, 7, 7, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 7, 8, 9);
    }






    /** Tests appding with correct prev_index, but incorrect term */
    public void testAppendWrongTermOnlyOneTerm() throws Exception {
        raft_a.currentTerm(22);
        for(int i=1; i <= 5; i++)
            add(rha, 1);
        assert sma.counter() == 5;
        assert smb.counter() == 4; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 4, 22, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 4, 5, 6);

        // now append beyond the end
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        int prev_index=9; // index == 10
        int term=raft_a.currentTerm(), commit_index=raft_a.commitIndex();



        // send a correct index, but incorrect prev_term:
        prev_index=5; // index == 6
        commit_index=raft_a.commitIndex();

        // send an AppendEntriesRequest with in incorrect index
        Message msg=new BytesMessage(null, val, 0, val.length)
          .putHeader(raft_a.getId(), new AppendEntriesRequest(raft_a.getAddress(),
                                                              prev_index, 23, 25, commit_index, false))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        raft_a.getDownProtocol().down(msg);
        Util.sleep(2000);
        assertCommitTableIndeces(b.getAddress(), raft_a, 4, 5, 6);

    }

    protected static void sendAppendEntriesRequest(RAFT r, int prev_index, int prev_term, int curr_term, int commit_index) {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        Message msg=new BytesMessage(null, val, 0, val.length)
          .putHeader(r.getId(), new AppendEntriesRequest(r.getAddress(),
                                                         prev_index, prev_term, curr_term, commit_index, false))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        r.getDownProtocol().down(msg);
    }


    protected static void assertIndices(int expected_last, int expected_commit, int expected_term, RAFT... rafts) {
        for(RAFT r: rafts) {
            assert r.lastAppended() == expected_last
              : String.format("RAFT.last_appended=%d, expected=%d", r.lastAppended(), expected_last);
            assert r.log().lastAppended() == expected_last
              : String.format("RAFT.log=%s, expected last=%d", r.log(), expected_last);

            assert r.commitIndex() == expected_commit
              : String.format("RAFT.commit=%d, expected=%d", r.commitIndex(), expected_commit);
            assert r.log().commitIndex() == expected_commit
              : String.format("RAFT.log=%s, expected commit=%d", r.commitIndex(), expected_commit);

            assert r.currentTerm() == expected_term
              : String.format("RAFT.term=%d, expected=%d", r.currentTerm(), expected_term);
            assert r.log().currentTerm() == expected_term
              : String.format("RAFT.log=%s, expected term=%d", r.log(), expected_term);
        }
    }

    protected static void assertCommitTableIndeces(Address member, RAFT r, int commit_index, int match_index, int next_index) {
        CommitTable table=r.commitTable();
        assert table != null;
        CommitTable.Entry e=table.get(member);
        assert e.commitIndex() == commit_index : String.format("expected commit_index=%d, entry=%s", commit_index, e);
        assert e.matchIndex() == match_index : String.format("expected match_index=%d, entry=%s", match_index, e);
        assert e.nextIndex() == next_index : String.format("expected next_index=%d, entry=%s", next_index, e);
    }

    protected static int add(RaftHandle rh, int delta) throws Exception {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(delta, val, 0);
        byte[] retval=rh.set(val, 0, val.length, 5, TimeUnit.MINUTES); // todo: change to 5 seconds
        return Bits.readInt(retval, 0);
    }

    protected static JChannel create(String name, long resend_interval, int max_log_size) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new NO_DUPES(),
          new GMS().setJoinTimeout(1000),
          new FRAG2(),
          // new ELECTION().electionMinInterval(100).electionMaxInterval(300).heartbeatInterval(30),
          new RAFT().members(List.of("A", "B", "C")).raftId(name)
            .logName("rafttest-" + name).resendInterval(resend_interval).maxLogSize(max_log_size),
          new REDIRECT()
        };
        return new JChannel(protocols).name(name);
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }
}
