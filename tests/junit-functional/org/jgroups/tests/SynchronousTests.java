package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.View;
import org.jgroups.protocols.raft.AppendEntriesRequest;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.RaftImpl;
import org.jgroups.raft.Options;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.stack.Protocol;
import org.jgroups.tests.harness.BaseRaftClusterTest;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Same as {@link RaftTest}, but only a single thread is used to run the tests (no asynchronous processing).
 * This allows for simple stepping-through in a debugger, without a thread handing off the processing to another
 * thread. Simplifies observing changes to the state machines represented by leaders and followers.
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="logProvider")
public class SynchronousTests extends BaseRaftClusterTest<RaftCluster> {
    protected View                view;
    protected static final int    TERM=5;
    private Class<? extends Log> logClass;

    {
        clusterSize = 3;
        recreatePerMethod = true;
        createManually = true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void passDataProviderParameters(Object[] args) {
        logClass = (Class<? extends Log>) args[0];
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.stateMachine(new CounterStateMachine())
                .synchronous(true)
                .resendInterval(600_00);
    }

    @Override
    protected String getRaftLogClass() {
        return logClass.getCanonicalName();
    }

    @Override
    protected Protocol[] baseProtocolStackForNode(String name) throws Exception {
        return new Protocol[] {
                createNewRaft(name),
        };
    }

    @DataProvider
    static Object[][] logProvider() {
        return new Object[][] {
                {LevelDBLog.class},
                {FileBasedLog.class}
        };
    }

    @BeforeMethod
    protected void init() throws Exception {
        createCluster(2);

        view = createView(1, 0, 1);
        cluster.handleView(view);

        raft(0).setLeaderAndTerm(address(0), TERM);
        raft(1).setLeaderAndTerm(address(0), TERM);
    }

    @AfterMethod(alwaysRun = true)
    protected void destroy() throws Exception {
        destroyCluster();
    }


    public void testSimpleAppend(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        int prev_value=add(1);
        assert prev_value == 0;
        assert stateMachine(0).counter() == 1;
        assert stateMachine(1).counter() == 0; // not yet committed

        prev_value=add(1);
        assert prev_value == 1;
        assert stateMachine(0).counter() == 2;
        assert stateMachine(1).counter() == 1; // not yet committed
    }


    public void testRegularAppend(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        int prev_value=add(1);
        expect(0, prev_value);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        assert sma.counter() == 1;
        assert smb.counter() == 0; // resend_interval is big, so the commit index on B won't get updated

        RAFT raft_a = raft(0);
        RAFT raft_b = raft(1);
        assertIndices(1, 1, TERM, raft(0));
        assertIndices(1, 0, TERM, raft(1));
        assertCommitTableIndeces(address(1), raft_a, 0, 1, 2);

        prev_value=add(2);
        assert prev_value == 1;
        assert sma.counter() == 3;
        assert smb.counter() == 1; // previous value; B is always lagging one commit behind
        assertCommitTableIndeces(address(1), raft_a, 1, 2, 3);

        prev_value=add(3);
        assert prev_value == 3;
        assert sma.counter() == 6;
        assert smb.counter() == 3; // previous value; B is always lagging one commit behind
        assertIndices(3, 3, TERM, raft_a);
        assertIndices(3, 2, TERM, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 2, 3, 4);

        prev_value=add(-3);
        assert prev_value == 6;
        assert sma.counter() == 3;
        assert smb.counter() == 6; // previous value; B is always lagging one commit behind
        assertIndices(4, 4, TERM, raft_a);
        assertIndices(4, 3, TERM, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 3, 4, 5);

        for(int i=1,prev=3; i <= 1000; i++) {
            prev_value=add(5);
            assert prev_value == prev;
            prev+=5;
        }
        assert sma.counter() == 5000 + 3;
        assert smb.counter() == sma.counter() - 5;

        assertIndices(1004, 1004, TERM, raft_a);
        assertIndices(1004, 1003, TERM, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 1003, 1004, 1005);

        long current_term=raft_a.currentTerm(), expected_term;
        raft_a.setLeaderAndTerm(address(0), expected_term=current_term + 10);

        for(int i=1; i <= 7; i++)
            add(1);

        assert sma.counter() == 5010;
        assert smb.counter() == sma.counter() - 1;

        assertIndices(1011, 1011, expected_term, raft_a);
        assertIndices(1011, 1010, expected_term, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 1010, 1011, 1012);
    }


    public void testAppendSameElementTwice(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 20);

        RAFT raft_b = raft(1);

        for(int i=1; i <= 5; i++)
            add(1);
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 4, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        expect(5, sma.counter());
        expect(4, smb.counter());

        // this will append the same entry, but because index 4 is < last_appended (5), it will not get appended
        // Note though that the commit-index will be updated
        sendAppendEntriesRequest(raft_a, 3, 20, 20, 5);
        Util.waitUntil(5000, 50, () -> raft_b.commitIndex() == 5);
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 5, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 5, 5, 6);
        assert sma.counter() == 5;
        assert smb.counter() == 5;
    }

    public void testAppendBeyondLast(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 22);
        for(int i=1; i <= 5; i++)
            add(1);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        assert sma.counter() == 5;
        assert smb.counter() == 4; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 4, 22, raft(1));
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);

        // now append beyond the end:
        sendAppendEntriesRequest(raft_a, 9, 22, 22, 0);
        // nothing changed, as request was rejected
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);
    }

    /** Tests appding with correct prev_index, but incorrect term */
    public void testAppendWrongTerm(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 22);

        RAFT raft_b = raft(1);

        for(int i=1; i <= 15; i++) {
            if(i % 5 == 0)
                raft_a.setLeaderAndTerm(address(0), raft_a.currentTerm()+1);
            add(1);
        }
        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        expect(15, sma.counter());
        expect(14, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(15, 15, 25, raft_a);
        assertIndices(15, 14, 25, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 14, 15, 16);

        // now append entries 16,17 and 18 (all with term=25), but *don't* advance the commit index
        for(int i=16; i <= 18; i++)
            sendAppendEntriesRequest(raft_a, i-1, 25, 25, 14);
        Util.waitUntil(5000, 100, () -> raft_b.lastAppended() == 18);
        assert sma.counter() == 15;
        assert smb.counter() == 14; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(15, 15, 25, raft_a);
        assertIndices(18, 14, 25, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 14, 18, 19);

        // send a correct index, but incorrect prev_term:
        int incorrect_prev_term=24;
        long commit_index=raft_a.commitIndex(), prev_index=18;

        raft_a.setLeaderAndTerm(address(0), 30);
        sendAppendEntriesRequest(raft_a, prev_index, incorrect_prev_term, 30, commit_index);
        assertCommitTableIndeces(address(1), raft_a, 14, 14, 15);

        // now apply the updates on the leader
        for(int i=16; i <= 18; i++)
            addAsync(raft_a,1, Options.create(false));
        raft_a.flushCommitTable(address(1)); // first time: get correct last_appended (18)
        raft_a.flushCommitTable(address(1)); // second time: send missing messages up to and including last_appended (18)
        // assertCommitTableIndeces(b, raft_a, 17, 18, 19);
        raft_a.flushCommitTable();
        assertCommitTableIndeces(address(1), raft_a, 18, 18, 19);

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
    public void testIncorrectAppend(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        int prev_value=add(1);
        assert prev_value == 0;
        assert sma.counter() == 1;
        assert smb.counter() == 0; // resend_interval is big, so the commit index on B won't get updated

        RAFT raft_a = raft(0);
        RAFT raft_b = raft(1);
        assertIndices(1, 1, 5, raft_a);
        assertIndices(1, 0, 5, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 0, 1, 2);

        raft_a.setLeaderAndTerm(address(0), 7);
        prev_value=add(1);
        assert prev_value == 1;
        prev_value=add(1);
        assert prev_value == 2;
        assert sma.counter() == 3;
        assert smb.counter() == 2; // previous value; B is always lagging one commit behind
        assertCommitTableIndeces(address(1), raft_a, 2, 3, 4);

        raft_a.setLeaderAndTerm(address(0), 9);
        for(int i=1; i <= 3; i++)
            add(1);

        long last_correct_append_index=raft_a.lastAppended(); // 6
        long index=last_correct_append_index+1;

        // add some incorrect entries with term=5 to B:
        RaftImpl impl=raft_b.impl();
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        // 7
        LogEntries entries=createLogEntries(10, val);
        AppendResult result=impl.handleAppendEntriesRequest(entries, address(0), index - 1, 9, 10, 1);
        assert result.success();
        raft_b.currentTerm(10);
        index++;

        // 8
        result=impl.handleAppendEntriesRequest(entries, address(0), index-1, 10, 10, 1);
        assert result.success();
        assertIndices(8, 5, 10, raft_b);

        raft_a.setLeaderAndTerm(address(0), 11);
        for(int i=1; i <= 2; i++)
            add(-1);

        // last_appended indices 7 and 8 need to be replaced with term=7
        assert sma.counter() == 4;
        assert smb.counter() == 5; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(8, 8, 11, raft_a);
        assertIndices(8, 7, 11, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 7, 8, 9);
    }


    /** Tests appending with correct prev_index, but incorrect term */
    public void testAppendWrongTermOnlyOneTerm(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 22);
        for(int i=1; i <= 5; i++)
            add(1);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        expect(5, sma.counter());
        expect(4, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 4, 22, raft(1));
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);

        // now append beyond the end
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        // send a correct index, but incorrect prev_term: does this ever happen? probably not, as the commit index
        // can never decrease!
        LogEntries entries=createLogEntries(25, val);
        Message msg=new ObjectMessage(null, entries)
          .putHeader(raft_a.getId(), new AppendEntriesRequest(raft_a.getAddress(), 22,
                                                              5, 23, 25, 0))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        raft_a.getDownProtocol().down(msg);
        raft_a.flushCommitTable(address(1));
        expect(5, sma.counter());
        expect(5, smb.counter()); // resend_interval is big, so the commit index on B won't get updated
        assertIndices(5, 5, 22, raft_a);
        assertIndices(5, 5, 22, raft(1));
        assertCommitTableIndeces(address(1), raft_a, 5, 5, 6);
    }

    public void testSnapshot(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_b = raft(1);
        raft_b.maxLogSize(100);
        for(int i=1; i <= 100; i++) {
            add(i);
            if(raft_b.numSnapshots() > 0)
                break;
        }

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        expect(325, sma.counter());
        expect(300, smb.counter());

        RaftNode node_b = node(1);
        node_b.stop();
        ((CounterStateMachine)raft_b.stateMachine()).reset();
        raft_b.stateMachineLoaded(false);
        raft_b.log(null); // log needs to be null, or else it on't get re-initialized
        node_b.start(); // reads snapshot and log

        expect(325, sma.counter());
        expect(300, smb.counter());
        raft(0).flushCommitTable(address(1));
        expect(325, sma.counter());
        expect(325, smb.counter());
    }

    /** Tests adding C to cluster A,B, transfer of state from A to C */
    public void testAddThirdMember(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 20);

        RAFT raft_b = raft(1);
        for(int i=1; i <= 5; i++)
            add(i);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        expect(15, sma.counter());
        expect(10, smb.counter());
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 4, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);

        addMemberC();
        raft_a.flushCommitTable(address(1));
        raft_a.flushCommitTable(address(2)); // sets C's next-index to 1
        raft_a.flushCommitTable(address(2)); // sends single append, next-index = 2
        raft_a.flushCommitTable(address(2)); // sends messages/commit 2-5
        expect(15, stateMachine(2).counter());
    }


    /** Members A,B,C: A and B have commit-index=10, B still has 5. A now snapshots its log at 10, but needs to
     * resend messages missed by C and cannot find them; therefore a snapshot is sent from A -> C
     */
    public void testSnapshotOnLeader(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 20);
        addMemberC();
        for(int i=1; i <= 5; i++)
            add(i);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        CounterStateMachine smc = stateMachine(2);
        expect(15, sma.counter());
        expect(10, smb.counter());
        expect(10, smc.counter());
        assertIndices(5, 5, 20, raft_a);

        RAFT raft_b = raft(1);
        RAFT raft_c = raft(2);
        assertIndices(5, 4, 20, raft_b);
        assertIndices(5, 4, 20, raft_c);
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);
        assertCommitTableIndeces(address(2), raft_a, 4, 5, 6);

        cluster.dropTrafficTo(address(2));
        for(int i=6; i <= 10; i++)
            add(i);
        expect(55, sma.counter());
        expect(45, smb.counter());
        expect(10, smc.counter());
        assertIndices(10, 10, 20, raft_a);
        assertIndices(10, 9, 20, raft_b);
        assertIndices(5, 4, 20, raft_c);
        assertCommitTableIndeces(address(1), raft_a, 9, 10, 11);
        assertCommitTableIndeces(address(2), raft_a, 4, 5, 6);

        // now snapshot the leader at 10, and resume traffic
        raft_a.snapshot();
        cluster.clearDroppedTraffic();

        raft_a.flushCommitTable();
        raft_a.flushCommitTable(address(2));
        expect(55, sma.counter());
        expect(55, smb.counter());
        expect(55, smc.counter());
        assertIndices(10, 10, 20, raft_a);
        assertIndices(10, 10, 20, raft_b);
        assertIndices(10, 10, 20, raft_c);
        assertCommitTableIndeces(address(1), raft_a, 10, 10, 11);
        assertCommitTableIndeces(address(2), raft_a, 10, 10, 11);
    }


    public void testSnapshotOnFollower(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 20);
        for(int i=1; i <= 5; i++)
            add(i);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        expect(15, sma.counter());
        expect(10, smb.counter());
        assertIndices(5, 5, 20, raft_a);

        RAFT raft_b = raft(1);
        assertIndices(5, 4, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);

        raft_b.snapshot();
        assertIndices(5, 4, 20, raft_b);

        raft_b.stop();
        raft_b.log(null); // required to re-initialize the log
        ((CounterStateMachine)raft_b.stateMachine()).reset();
        raft_b.stateMachineLoaded(false);
        raft_b.start();
        expect(10, smb.counter());
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 4, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);

        for(int i=6; i <= 10; i++)
            add(i);
        expect(55, sma.counter());
        expect(45, smb.counter());
        assertIndices(10, 10, 20, raft_a);
        assertIndices(10, 9, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 9, 10, 11);

        raft_a.flushCommitTable(address(1));
        expect(55, smb.counter());
        assertIndices(10, 10, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 10, 10, 11);
    }

    public void testSnapshotSentToFollower(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        raft_a.setLeaderAndTerm(address(0), 20);
        for(int i=1; i <= 5; i++)
            add(i);

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        expect(15, sma.counter());
        expect(10, smb.counter());
        assertIndices(5, 5, 20, raft_a);

        RAFT raft_b = raft(1);
        assertIndices(5, 4, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 4, 5, 6);

        RaftNode node_b = node(1);
        raft_b.stop();
        RaftTestUtils.deleteRaftLog(raft_b);
        raft_b.log(null); // required to re-initialize the log
        ((CounterStateMachine)raft_b.stateMachine()).reset();
        raft_b.stateMachineLoaded(false);

        raft_a.snapshot();
        assertIndices(5, 5, 20, raft_a);

        view=createView(view.getViewId().getId()+1, 0);
        cluster.handleView(view);

        cluster.add(raft_b.getAddress(), node_b);
        raft_b.start();
        view=createView(view.getViewId().getId()+1, 0, 1);
        cluster.handleView(view);

        raft_a.flushCommitTable(address(1)); // first flush will set next-index to 1
        raft_a.flushCommitTable(address(1)); // this flush will send the snapshot from A to B
        expect(15, sma.counter());
        expect(15, smb.counter());
        assertIndices(5, 5, 20, raft_a);
        assertIndices(5, 5, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 5, 5, 6);

        for(int i=6; i <= 10; i++)
            add(i);
        expect(55, sma.counter());
        expect(45, smb.counter());
        assertIndices(10, 10, 20, raft_a);
        assertIndices(10, 9, 20, raft_b);
        assertCommitTableIndeces(address(1), raft_a, 9, 10, 11);
    }

    public void testIgnoreResponse(@SuppressWarnings("unused") Class<?> logClass) throws Exception {
        RAFT raft_a = raft(0);
        CompletableFuture<byte[]> f=addAsync(raft_a, 5, Options.create(false));
        assert f != null;
        int prev_value=Bits.readInt(f.get(), 0);
        assert prev_value == 0;

        CounterStateMachine sma = stateMachine(0);
        CounterStateMachine smb = stateMachine(1);
        assert sma.counter() == 5;
        assert smb.counter() == 0;

        f=addAsync(raft_a, 5, Options.create(false));
        byte[] rsp=f.get();
        prev_value=Bits.readInt(rsp, 0);
        assert prev_value == 5;
        assert sma.counter() == 10;
        assert smb.counter() == 5;

        f=addAsync(raft_a, 5, Options.create(true));
        assert f != null;
        rsp=f.get();
        assert rsp == null;
        assert sma.counter() == 15;
        assert smb.counter() == 10;
    }

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }

    protected static byte[] num(int n) {
        byte[] buf=new byte[Integer.BYTES];
        Bits.writeInt(n, buf, 0);
        return buf;
    }

    protected int add(int delta) throws Exception {
        byte[] buf=num(delta);
        CompletableFuture<byte[]> f=raft(0).setAsync(buf, 0, buf.length);
        byte[] retval=f.get(10, TimeUnit.SECONDS);
        return Bits.readInt(retval, 0);
    }

    protected static CompletableFuture<byte[]> addAsync(RAFT r, int delta, Options opts) throws Exception {
        byte[] buf=num(delta);
        return r.setAsync(buf, 0, buf.length, opts);
    }

    protected static void sendAppendEntriesRequest(RAFT r, long prev_index, long prev_term, long curr_term, long commit_index) {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(1, val, 0);

        LogEntries entries=createLogEntries(curr_term, val);
        Message msg=new ObjectMessage(null, entries)
          .putHeader(r.getId(), new AppendEntriesRequest(r.getAddress(), curr_term,
                                                         prev_index, prev_term, curr_term, commit_index))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        r.getDownProtocol().down(msg);
    }

    protected static void assertIndices(long expected_last, long expected_commit, long expected_term, RAFT... rafts) {
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

    protected static LogEntries createLogEntries(long curr_term, byte[] buf) {
        return new LogEntries().add(new LogEntry(curr_term, buf));
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

    protected void addMemberC() throws Exception {
        createCluster();
        view = createView(2, 0, 1, 2);
        cluster.handleView(view);
    }

    private CounterStateMachine stateMachine(int index) {
        RAFT r = raft(index);
        assertThat(r).isNotNull();
        return (CounterStateMachine) r.stateMachine();
    }
}
