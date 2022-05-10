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
import org.jgroups.raft.Options;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Tests the various stages of the Raft protocoll, e.g. regular append, incorrect append, snapshots, leader change,
 * leader election etc
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RaftTest {
    protected JChannel            a, b;
    protected RaftHandle          rha, rhb;
    protected RAFT                raft_a, raft_b;
    protected CounterStateMachine sma, smb;
    protected static final String GRP="RaftTest";

    @BeforeMethod
    protected void create() throws Exception {
        a=create("A", 600_000, 1_000_000);
        rha=new RaftHandle(a, sma=new CounterStateMachine());
        a.connect(GRP);
        raft_a=raft(a).setLeaderAndTerm(a.getAddress());

        b=create("B", 600_000, 1_000_000);
        rhb=new RaftHandle(b, smb=new CounterStateMachine());
        b.connect(GRP);
        raft_b=raft(b).setLeaderAndTerm(a.getAddress());
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
        assert raft_a.isLeader();
        assert !raft_b.isLeader();
    }

    @AfterMethod protected void destroy() throws Exception {
        for(JChannel ch: Arrays.asList(b, a)) {
            if(ch == null)
                continue;
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            raft.log().delete();
            Util.close(ch);
        }
    }


    public void testRegularAppend() throws Exception {
        int prev_value=add(rha, 1);
        expect(0, prev_value);
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
        expect(5, sma.counter());
        expect(4, smb.counter());

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
        expect(15, sma.counter());
        assert smb.counter() == 14; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(15, 15, 25, raft_a);
        assertIndices(15, 14, 25, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 14, 15, 16);

        // now append entries 16,17 and 18 (all with term=25), but *don't* advance the commit index
        for(int i=16; i <= 18; i++)
            sendAppendEntriesRequest(raft_a, i-1, 25, 25, 14);
        Util.waitUntil(5000, 100, () -> raft_b.lastAppended() == 18);
        assert sma.counter() == 15;
        assert smb.counter() == 14; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(15, 15, 25, raft_a);
        assertIndices(18, 14, 25, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 14, 18, 19);


        // send a correct index, but incorrect prev_term:
        int incorrect_prev_term=24;
        int commit_index=raft_a.commitIndex(), prev_index=18;

        sendAppendEntriesRequest(raft_a, prev_index, incorrect_prev_term, 30, commit_index);
        Util.sleep(1000); // nothing changed
        assertCommitTableIndeces(b.getAddress(), raft_a, 14, 14, 15);

        // now apply the updates on the leader
        raft_a.currentTerm(30);
        raft_a.resendInterval(1000);
        for(int i=16; i <= 18; i++)
            add(rha, 1);
        Util.waitUntil(5000, 100, () -> raft_b.lastAppended() == 18);
        assertCommitTableIndeces(b.getAddress(), raft_a, 17, 18, 19);

        raft_a.flushCommitTable();
        Util.waitUntil(5000, 100, () -> raft_b.commitIndex() == 18);

        assertCommitTableIndeces(b.getAddress(), raft_a, 18, 18, 19);

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
        LogEntries entries=new LogEntries().add(new LogEntry(5, val));
        AppendResult result=impl.handleAppendEntriesRequest(entries, a.getAddress(), index - 1, 4, 5, 1);
        assert result.success();
        raft_b.currentTerm(5);
        index++;

        // 8
        result=impl.handleAppendEntriesRequest(entries, a.getAddress(), index-1, 5, 5, 1);
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


    /** Tests appending with correct prev_index, but incorrect term */
    public void testAppendWrongTermOnlyOneTerm() throws Exception {

        raft_a.resendInterval(1000);

        raft_a.currentTerm(22);
        for(int i=1; i <= 5; i++)
            add(rha, 1);
        expect(5, sma.counter());
        expect(4, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 4, 22, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 4, 5, 6);

        // now append beyond the end
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        // send a correct index, but incorrect prev_term: does this ever happen?
        LogEntries entries=new LogEntries().add(new LogEntry(25, val));
        Message msg=new ObjectMessage(null, entries)
          .putHeader(raft_a.getId(), new AppendEntriesRequest(raft_a.getAddress(), 22,
                                                              5, 23, 25, 0))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        raft_a.getDownProtocol().down(msg);
        Util.waitUntilTrue(5000, 200, () -> raft_b.commitIndex() == 5);
        expect(5, sma.counter());
        expect(5, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 5, 22, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 5, 5, 6);
    }


    public void testSimpleAppendOnFollower() throws Exception {
        CompletableFuture<byte[]> f=addAsync(rhb, 5);
        assert f != null;
        int prev_value=Bits.readInt(f.get(), 0);
        assert prev_value == 0;
        assert sma.counter() == 5;
        assert smb.counter() == 0; // not yet committed

        f=addAsync(rhb, 5);
        prev_value=Bits.readInt(f.get(), 0);
        assert prev_value == 5;
        assert sma.counter() == 10;
        assert smb.counter() == 5; // not yet committed

        f=addAsync(rhb, 5, Options.create(true));
        assert f != null;
        byte[] res=f.get();
        assert res == null;
        assert sma.counter() == 15;
        assert smb.counter() == 10; // not yet committed
    }


    protected static void sendAppendEntriesRequest(RAFT r, int prev_index, int prev_term, int curr_term, int commit_index) {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        LogEntries entries=new LogEntries().add(new LogEntry(curr_term, val));
        Message msg=new ObjectMessage(null, entries)
          .putHeader(r.getId(), new AppendEntriesRequest(r.getAddress(), curr_term,
                                                         prev_index, prev_term, curr_term, commit_index))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        r.getDownProtocol().down(msg);
    }


    protected static void assertIndices(int expected_last, int expected_commit, int expected_term, RAFT r) {
        Util.waitUntilTrue(1000, 50, () -> r.lastAppended() == expected_last &&
          r.commitIndex() == expected_commit && r.currentTerm() == expected_term &&
          r.log().lastAppended() == expected_last && r.log().commitIndex() == expected_commit);
        assert r.lastAppended() == expected_last
          : String.format("RAFT.last_appended=%d, expected=%d", r.lastAppended(), expected_last);
        assert r.log().lastAppended() == expected_last
          : String.format("RAFT.log=%s, expected last=%d", r.log(), expected_last);

        assert r.commitIndex() == expected_commit
          : String.format("RAFT.commit=%d, expected=%d", r.commitIndex(), expected_commit);
        assert r.log().commitIndex() == expected_commit
          : String.format("RAFT.log.commit-index=%d, expected commit=%d", r.log().commitIndex(), expected_commit);

        assert r.currentTerm() == expected_term
          : String.format("RAFT.term=%d, expected=%d", r.currentTerm(), expected_term);
        assert r.log().currentTerm() == expected_term
          : String.format("RAFT.log=%s, expected term=%d", r.log(), expected_term);
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

    protected static int add(RaftHandle rh, int delta) throws Exception {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(delta, val, 0);
        byte[] retval=rh.set(val, 0, val.length, 5, TimeUnit.SECONDS);
        return Bits.readInt(retval, 0);
    }

    protected static CompletableFuture<byte[]> addAsync(RaftHandle handle, int delta) throws Exception {
        return addAsync(handle, delta, null);
    }

    protected static CompletableFuture<byte[]> addAsync(RaftHandle handle, int delta, Options opts) throws Exception {
        byte[] buf=new byte[Integer.BYTES];
        Bits.writeInt(delta, buf, 0);
        return handle.setAsync(buf, 0, buf.length, opts);
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
            .logPrefix("rafttest-" + name).resendInterval(resend_interval).maxLogSize(max_log_size),
          new REDIRECT()
        };
        return new JChannel(protocols).name(name);
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }

    protected static void expect(int expected_value, int actual_value) {
        assert actual_value == expected_value : String.format("expected=%d actual=%d", expected_value, actual_value);
    }
}
