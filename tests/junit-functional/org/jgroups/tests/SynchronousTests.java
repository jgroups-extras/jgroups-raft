package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.raft.*;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.util.Bits;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Same as {@link RaftTest}, but only a single thread is used to run the tests (no asynchronous processing).
 * This allows for simple stepping-through in a debugger, without a thread handing off the processing to another
 * thread. Simplifies observing changes to the state machines represented by leaders and followers.
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class SynchronousTests {
    protected final Address       a=createAddress("A"), b=createAddress("B");
    protected final View          view=View.create(a, 1, a,b);
    protected final List<String>  mbrs=List.of("A", "B");
    protected final RaftCluster   cluster=new RaftCluster();
    protected RAFT                raft_a, raft_b;
    protected RaftNode            node_a, node_b;
    protected CounterStateMachine sma, smb;
    protected static final int    TERM=5;

    @BeforeMethod protected void init() throws Exception {
        raft_a=createRAFT(a, "A", mbrs).stateMachine(sma=new CounterStateMachine())
          .changeRole(Role.Leader).leader(a);
        raft_b=createRAFT(b, "B", mbrs).stateMachine(smb=new CounterStateMachine())
          .changeRole(Role.Follower).leader(a);
        node_a=new RaftNode(cluster, raft_a);
        node_b=new RaftNode(cluster, raft_b);
        node_a.init();
        node_b.init();
        cluster.add(a, node_a).add(b, node_b);
        cluster.handleView(view);
        node_a.start();
        node_b.start();
        raft_a.currentTerm(TERM);
    }


    @AfterMethod
    protected void destroy() throws Exception {
        node_b.stop();
        node_a.stop();
        node_b.destroy();
        node_a.destroy();
        raft_a.deleteLog();
        raft_a.deleteSnapshot();
        raft_b.deleteLog();
        raft_b.deleteSnapshot();
        cluster.clear();
    }


    public void testSimpleAppend() throws Exception {
        int prev_value=add(1);
        assert prev_value == 0;
        assert sma.counter() == 1;
        assert smb.counter() == 0; // not yet committed

        prev_value=add(1);
        assert prev_value == 1;
        assert sma.counter() == 2;
        assert smb.counter() == 1; // not yet committed
    }


    public void testRegularAppend() throws Exception {
        int prev_value=add(1);
        expect(0, prev_value);
        assert sma.counter() == 1;
        assert smb.counter() == 0; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(1, 1, TERM, raft_a);
        assertIndices(1, 0, TERM, raft_b);
        assertCommitTableIndeces(b, raft_a, 0, 1, 2);

        prev_value=add(2);
        assert prev_value == 1;
        assert sma.counter() == 3;
        assert smb.counter() == 1; // previous value; B is always lagging one commit behind
        assertCommitTableIndeces(b, raft_a, 1, 2, 3);

        prev_value=add(3);
        assert prev_value == 3;
        assert sma.counter() == 6;
        assert smb.counter() == 3; // previous value; B is always lagging one commit behind
        assertIndices(3, 3, TERM, raft_a);
        assertIndices(3, 2, TERM, raft_b);
        assertCommitTableIndeces(b, raft_a, 2, 3, 4);

        prev_value=add(-3);
        assert prev_value == 6;
        assert sma.counter() == 3;
        assert smb.counter() == 6; // previous value; B is always lagging one commit behind
        assertIndices(4, 4, TERM, raft_a);
        assertIndices(4, 3, TERM, raft_b);
        assertCommitTableIndeces(b, raft_a, 3, 4, 5);

        for(int i=1,prev=3; i <= 1000; i++) {
            prev_value=add(5);
            assert prev_value == prev;
            prev+=5;
        }
        assert sma.counter() == 5000 + 3;
        assert smb.counter() == sma.counter() - 5;

        assertIndices(1004, 1004, TERM, raft_a);
        assertIndices(1004, 1003, TERM, raft_b);
        assertCommitTableIndeces(b, raft_a, 1003, 1004, 1005);

        int current_term=raft_a.currentTerm(), expected_term;
        raft_a.currentTerm(expected_term=current_term + 10);

        for(int i=1; i <= 7; i++)
            add(1);

        assert sma.counter() == 5010;
        assert smb.counter() == sma.counter() - 1;

        assertIndices(1011, 1011, expected_term, raft_a);
        assertIndices(1011, 1010, expected_term, raft_b);
        assertCommitTableIndeces(b, raft_a, 1010, 1011, 1012);
    }


    public void testAppendSameElementTwice() throws Exception {
        raft_a.currentTerm(20);
        for(int i=1; i <= 5; i++)
            add(1);
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 4, 20, raft_b);
        assertCommitTableIndeces(b, raft_a, 4, 5, 6);
        expect(5, sma.counter());
        expect(4, smb.counter());

        // this will append the same entry, but because index 4 is < last_appended (5), it will not get appended
        // Note though that the commit-index will be updated
        sendAppendEntriesRequest(raft_a, 3, 20, 20, 5);
        Util.waitUntil(5000, 50, () -> raft_b.commitIndex() == 5);
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 5, 20, raft_b);
        assertCommitTableIndeces(b, raft_a, 5, 5, 6);
        assert sma.counter() == 5;
        assert smb.counter() == 5;
    }

    public void testAppendBeyondLast() throws Exception {
        raft_a.currentTerm(22);
        for(int i=1; i <= 5; i++)
            add(1);
        assert sma.counter() == 5;
        assert smb.counter() == 4; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 4, 22, raft_b);
        assertCommitTableIndeces(b, raft_a, 4, 5, 6);

        // now append beyond the end:
        sendAppendEntriesRequest(raft_a, 9, 22, 22, 0);
        // nothing changed, as request was rejected
        assertCommitTableIndeces(b, raft_a, 4, 5, 6);
    }

    /** Tests appding with correct prev_index, but incorrect term */
    public void testAppendWrongTerm() throws Exception {
        raft_a.currentTerm(22);
        for(int i=1; i <= 15; i++) {
            if(i % 5 == 0)
                raft_a.currentTerm(raft_a.currentTerm()+1);
            add(1);
        }
        expect(15, sma.counter());
        expect(14, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(15, 15, 25, raft_a);
        assertIndices(15, 14, 25, raft_b);
        assertCommitTableIndeces(b, raft_a, 14, 15, 16);

        // now append entries 16,17 and 18 (all with term=25), but *don't* advance the commit index
        for(int i=16; i <= 18; i++)
            sendAppendEntriesRequest(raft_a, i-1, 25, 25, 14);
        Util.waitUntil(5000, 100, () -> raft_b.lastAppended() == 18);
        assert sma.counter() == 15;
        assert smb.counter() == 14; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(15, 15, 25, raft_a);
        assertIndices(18, 14, 25, raft_b);
        assertCommitTableIndeces(b, raft_a, 14, 18, 19);

        // send a correct index, but incorrect prev_term:
        int incorrect_prev_term=24;
        int commit_index=raft_a.commitIndex(), prev_index=18;

        sendAppendEntriesRequest(raft_a, prev_index, incorrect_prev_term, 30, commit_index);
        assertCommitTableIndeces(b, raft_a, 14, 14, 15);

        // now apply the updates on the leader
        raft_a.currentTerm(30);
        for(int i=16; i <= 18; i++)
            addAsync(1);
        raft_a.flushCommitTable(b); // first time: get correct last_appended (18)
        raft_a.flushCommitTable(b); // second time: send missing messages up to and including last_appended (18)
        assertCommitTableIndeces(b, raft_a, 17, 18, 19);

        raft_a.flushCommitTable();
        assertCommitTableIndeces(b, raft_a, 18, 18, 19);

        // compare the log entries from 1-18
        for(int i=0; i <= raft_a.lastAppended(); i++) {
            LogEntry la=raft_a.log().get(i), lb=raft_b.log().get(i);
            if(i == 0)
                assert la == null && lb == null;
            else {
                System.out.printf("%d: A=%s, B=%s\n", i, la, lb);
                assert la.term() == lb.term();
            }
        }
    }


    /** Tests appends where we change prev_term, so that we'll get an AppendResult with success=false */
    public void testIncorrectAppend() throws Exception {
        int prev_value=add(1);
        assert prev_value == 0;
        assert sma.counter() == 1;
        assert smb.counter() == 0; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(1, 1, 5, raft_a);
        assertIndices(1, 0, 5, raft_b);
        assertCommitTableIndeces(b, raft_a, 0, 1, 2);

        raft_a.currentTerm(7);
        prev_value=add(1);
        assert prev_value == 1;
        prev_value=add(1);
        assert prev_value == 2;
        assert sma.counter() == 3;
        assert smb.counter() == 2; // previous value; B is always lagging one commit behind
        assertCommitTableIndeces(b, raft_a, 2, 3, 4);

        raft_a.currentTerm(9);
        for(int i=1; i <= 3; i++)
            add(1);

        int last_correct_append_index=raft_a.lastAppended(); // 6
        int index=last_correct_append_index+1;

        // add some incorrect entries with term=5 to B:
        RaftImpl impl=raft_b.impl();
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        // 7
        AppendResult result=impl.handleAppendEntriesRequest(val, 0, val.length, a, index - 1, 9, 10, 1, false);
        assert result.success();
        raft_b.currentTerm(10);
        index++;

        // 8
        result=impl.handleAppendEntriesRequest(val, 0, val.length, a, index-1, 10, 10, 1, false);
        assert result.success();
        assertIndices(8, 5, 10, raft_b);

        raft_a.currentTerm(11);
        for(int i=1; i <= 2; i++)
            add(-1);

        // last_appended indices 7 and 8 need to be replaced with term=7
        assert sma.counter() == 4;
        assert smb.counter() == 5; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(8, 8, 11, raft_a);
        assertIndices(8, 7, 11, raft_b);
        assertCommitTableIndeces(b, raft_a, 7, 8, 9);
    }


    /** Tests appending with correct prev_index, but incorrect term */
    public void testAppendWrongTermOnlyOneTerm() throws Exception {
        raft_a.currentTerm(22);
        for(int i=1; i <= 5; i++)
            add(1);
        expect(5, sma.counter());
        expect(4, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 4, 22, raft_b);
        assertCommitTableIndeces(b, raft_a, 4, 5, 6);

        // now append beyond the end
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        // send a correct index, but incorrect prev_term: does this ever happen? probably not, as the commit index
        // can never decrease!
        Message msg=new BytesMessage(null, val, 0, val.length)
          .putHeader(raft_a.getId(), new AppendEntriesRequest(raft_a.getAddress(), 22,
                                                              5, 23, 25, 0, false))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        raft_a.getDownProtocol().down(msg);
        raft_a.flushCommitTable(b);
        expect(5, sma.counter());
        expect(5, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 5, 22, raft_b);
        assertCommitTableIndeces(b, raft_a, 5, 5, 6);

    }



    protected static RAFT createRAFT(Address addr, String name, List<String> members) {
        return new RAFT().raftId(name).members(members).logName("synctest-" + name)
          .resendInterval(600_000) // long to disable resending by default
          .setAddress(addr);
    }

    protected static Address createAddress(String name) {
        ExtendedUUID.setPrintFunction(RAFT.print_function);
        return ExtendedUUID.randomUUID(name).put(RAFT.raft_id_key, Util.stringToBytes(name));
    }

    protected static byte[] num(int n) {
        byte[] buf=new byte[Integer.BYTES];
        Bits.writeInt(n, buf, 0);
        return buf;
    }

    protected int add(int delta) throws Exception {
        byte[] buf=num(delta);
        CompletableFuture<byte[]> f=raft_a.setAsync(buf, 0, buf.length, null, true);
        byte[] retval=f.get(10, TimeUnit.SECONDS);
        return Bits.readInt(retval, 0);
    }

    protected void addAsync(int delta) throws Exception {
        byte[] buf=num(delta);
        raft_a.setAsync(buf, 0, buf.length, null, true);
    }

    protected static void sendAppendEntriesRequest(RAFT r, int prev_index, int prev_term, int curr_term, int commit_index) {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        Message msg=new BytesMessage(null, val, 0, val.length)
          .putHeader(r.getId(), new AppendEntriesRequest(r.getAddress(), curr_term,
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
        assert e != null;
        Util.waitUntilTrue(2000, 100,
                           () -> e.commitIndex() == commit_index && e.matchIndex() == match_index && e.nextIndex() == next_index);
        assert e.commitIndex() == commit_index : String.format("expected commit_index=%d, entry=%s", commit_index, e);
        assert e.matchIndex() == match_index : String.format("expected match_index=%d, entry=%s", match_index, e);
        assert e.nextIndex() == next_index : String.format("expected next_index=%d, entry=%s", next_index, e);
    }

    protected static void expect(int expected_value, int actual_value) {
        assert actual_value == expected_value : String.format("expected=%d actual=%d", expected_value, actual_value);
    }

}
