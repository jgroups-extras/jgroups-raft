 package org.jgroups.tests;

 import org.jgroups.Address;
 import org.jgroups.Global;
 import org.jgroups.JChannel;
 import org.jgroups.protocols.DISCARD;
 import org.jgroups.protocols.TP;
 import org.jgroups.protocols.raft.*;
 import org.jgroups.raft.RaftHandle;
 import org.jgroups.raft.blocks.ReplicatedStateMachine;
 import org.jgroups.stack.ProtocolStack;
 import org.jgroups.util.Util;
 import org.testng.annotations.AfterMethod;
 import org.testng.annotations.Test;

 import java.io.DataInput;
 import java.io.DataOutput;
 import java.lang.reflect.Field;
 import java.lang.reflect.Method;
 import java.util.Arrays;
 import java.util.Collections;
 import java.util.List;
 import java.util.Objects;
 import java.util.concurrent.TimeUnit;
 import java.util.concurrent.TimeoutException;
 import java.util.function.Function;
 import java.util.stream.Stream;

 import static org.testng.Assert.*;

/**
 * Tests the AppendEntries functionality: appending log entries in regular operation, new members, late joiners etc
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class AppendEntriesTest {
    protected JChannel                                a,  b,  c;  // A is always the leader
    protected ReplicatedStateMachine<Integer,Integer> as, bs, cs;
    protected static final Method                     handleAppendEntriesRequest;
    protected static final String                     CLUSTER="AppendEntriesTest";
    protected static final List<String>               members=Arrays.asList("A", "B", "C");

    static {
        try {
            handleAppendEntriesRequest=RaftImpl.class.getDeclaredMethod("handleAppendEntriesRequest",
                                                                        byte[].class, int.class, int.class, Address.class,
                                                                        int.class, int.class, int.class, int.class, boolean.class);
            handleAppendEntriesRequest.setAccessible(true);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    @AfterMethod
    protected void destroy() {
        close(true, true, c, b, a);
    }

    public void testSingleMember() throws Exception  {
        // given
        a=create("A", false, Collections.singletonList("A"));
        a.connect(CLUSTER);
        RaftHandle raft=new RaftHandle(a, new DummyStateMachine());
        Util.waitUntil(1000, 250, raft::isLeader);

        // when
        byte[] data="foo".getBytes();
        byte[] result=raft.set(data, 0, data.length, 1, TimeUnit.SECONDS);

        // then
        assert Arrays.equals(result, new byte[0]) : "should have received empty byte array";
    }

    public void testNormalOperation() throws Exception {
        init(true);
        for(int i=1; i <= 10; i++)
            as.put(i, i);
        assertSame(as, bs, cs);
        bs.remove(5);
        cs.put(11, 11);
        cs.remove(1);
        as.put(1, 1);
        assertSame(as, bs, cs);
    }


    public void testRedirect() throws Exception {
        init(true);
        cs.put(5, 5);
        assertSame(as, bs, cs);
    }


    public void testPutWithoutLeader() throws Exception {
        a=create("A", false); // leader
        as=new ReplicatedStateMachine<>(a);
        a.connect(CLUSTER);
        assert !isLeader(a);
        try {
            as.put(1, 1);
            assert false : "put() should fail as we don't have a leader";
        }
        catch(Throwable t) {
            System.out.println("received exception as expected: " + t);
        }
    }


    /**
     * The leader has no majority in the cluster and therefore cannot commit changes. The effect is that all puts on the
     * state machine will fail and the replicated state machine will be empty.
     */
    public void testNonCommitWithoutMajority() throws Exception {
        init(true);
        close(true, true, b, c);
        as.timeout(500);

        for(int i=1; i <= 3; i++) {
            try {
                as.put(i, i);
            }
            catch(Exception ex) {
                System.out.printf("received %s as expected; cache size is %d\n", ex.getClass().getSimpleName(), as.size());
            }
            assert as.size() == 0;
        }
    }

    /**
     * Leader A and followers B and C commit entries 1-2. Then C leaves and A and B commit entries 3-5. When C rejoins,
     * it should get log entries 3-5 as well.
     */
    public void testCatchingUp() throws Exception {
        init(true);
        // A, B and C commit entries 1-2
        for(int i=1; i <= 2; i++)
            as.put(i,i);
        assertSame(as, bs, cs);

        // Now C leaves
        close(true, true, c);

        // A and B commit entries 3-5
        for(int i=3; i <= 5; i++)
            as.put(i,i);
        assertSame(as, bs);

        // Now start C again: entries 1-5 will have to get resent to C as its log was deleted above (otherwise only 3-5
        // would have to be resent)
        c=create("C", true);  // follower
        cs=new ReplicatedStateMachine<>(c);
        c.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);

        // Now C should also have the same entries (1-5) as A and B
        assertSame(as, bs, cs);
    }

    /**
     * Leader A adds the first entry to its log but cannot commit it because it doesn't have a majority (B and C
     * discard all traffic). Then, the DISCARD protocol is removed and the commit entries of B and C should catch up.
     */
    public void testCatchingUpFirstEntry() throws Exception {
        // Create {A,B,C}, A is the leader, then close B and C (A is still the leader)
        init(false);

        // make B and C drop all traffic; this means A won't be able to commit
        for(JChannel ch: Arrays.asList(b,c)) {
            ProtocolStack stack=ch.getProtocolStack();
            DISCARD discard=new DISCARD().discardAll(true);
            stack.insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
        };

        // Add the first entry, this will time out as there's no majority
        as.timeout(500);
        try {
            as.put(1, 1);
            assert false : "should have gotten a TimeoutException";
        }
        catch(TimeoutException ignored) {
            System.out.println("The first put() timed out as expected as there's no majority to commit it");
        }

        RAFT raft=a.getProtocolStack().findProtocol(RAFT.class);
        System.out.printf("A: last-applied=%d, commit-index=%d\n", raft.lastAppended(), raft.commitIndex());
        assert raft.lastAppended() == 1;
        assert raft.commitIndex() == 0;

        // now remove the DISCARD protocol from B and C
        Stream.of(b,c).forEach(ch -> ch.getProtocolStack().removeProtocol(DISCARD.class));

        assertCommitIndex(10000, 500, raft.lastAppended(), raft.lastAppended(), a, b);
        for(JChannel ch: Arrays.asList(a,b)) {
            raft=ch.getProtocolStack().findProtocol(RAFT.class);
            System.out.printf("%s: last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
            assert raft.lastAppended() == 1;
            assert raft.commitIndex() == 1;
        }
        assertSame(as, bs);
    }

    /**
     * Tests https://github.com/belaban/jgroups-raft/issues/30-31: correct commit_index after leader restart, and
     * populating request table in RAFT on leader change
     */
    public void testLeaderRestart() throws Exception {
        a=create("A", false);
        raft(a).stateMachine(new DummyStateMachine());
        b=create("B", true);
        raft(b).stateMachine(new DummyStateMachine());
        a.connect(CLUSTER);
        b.connect(CLUSTER);
        // A and B now have a majority and A is leader
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        assertLeader(a, 10000, 500);
        assert !raft(b).isLeader();
        System.out.println("--> disconnecting B");
        b.disconnect(); // stop B; it was only needed to make A the leader

        // Now try to make a change on A. This will fail as A is not leader anymore
        try {
            raft(a).set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS);
            assert false : "set() should have thrown a timeout as we cannot commit the change";
        }
        catch(IllegalStateException ex) {
            System.out.printf("got exception as expected: %s\n", ex);
        }

        // A now has last_applied=1 and commit_index=0:
        assertCommitIndex(10000, 500, 0, 0, a);

        // Now start B again, this gives us a majority and entry #1 should be able to be committed
        System.out.println("--> restarting B");
        b=create("B", true);
        raft(b).stateMachine(new DummyStateMachine());
        b.connect(CLUSTER);
        // A and B now have a majority and A is leader
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        // A and B should now have last_applied=0 and commit_index=0
        assertCommitIndex(10000, 500, 0, 0, a,b);
    }


    /**
     * Leader A and follower B commit 5 entries, then snapshot A. Then C comes up and should get the 5 committed entries
     * as well, as a snapshot
     */
    public void testInstallSnapshotInC() throws Exception {
        init(true);
        close(true, true, c);
        for(int i=1; i <= 5; i++)
            as.put(i,i);
        assertSame(as, bs);

        // Snapshot A:
        as.snapshot();

        // Now start C
        c=create("C", true);  // follower
        cs=new ReplicatedStateMachine<>(c);
        c.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);

        assertSame(as, bs, cs);
    }


    /** Tests an append at index 1 with prev_index 0 and index=2 with prev_index=1*/
    public void testInitialAppends() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        byte[] buf=new byte[10];
        AppendResult result=append(impl, 1, 0, new LogEntry(4, buf), leader, 1);
        assert result.success();
        assertEquals(result.index(), 1);
        assertEquals(result.commitIndex(), 1);
        assertLogIndices(log, 1, 1, 4);

        result=append(impl, 2, 4, new LogEntry(4, buf), leader, 1);
        assert result.success();
        assertEquals(result.index(), 2);
        assertEquals(result.commitIndex(), 1);
        assertLogIndices(log, 2, 1, 4);
    }

    public void testIncorrectAppend() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        byte[] buf=new byte[10];

        // initial append at index 1
        AppendResult result=append(impl, 1, 0, new LogEntry(4, buf), leader, 1);
        assert result.success();
        assertEquals(result.index(), 1);
        assertLogIndices(log, 1, 1, 4);

        // append at index 3 fails because there is no entry at index 2
        result=append(impl, 3, 4, new LogEntry(4, buf), leader, 1);
        assert !result.success();
        assertEquals(result.index(), 1);
        assertLogIndices(log, 1, 1, 4);

        // append at index 2 with term 3 fails as prev-term is 4
        result=append(impl, 2, 3, new LogEntry(4, buf), leader, 1);
        assert !result.success();
        assertEquals(result.index(), 1);
        assertLogIndices(log, 1, 1, 4);
    }

    public void testSendCommitsImmediately() throws Exception {
        // Force leader to send commit messages immediately
        init(true, r -> r.resendInterval(1000000000).sendCommitsImmediately(true));
        as.put(1,1);
        try {
            assertSame(as, bs); // if as is not equal to bs, `as` should then be equals to `cs`
        } catch (AssertionError e) {
            assertSame(as, cs);
        }
    }

    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Log    01 01 01 04 04 05 05 06 06 06
    // Append                            07 <--- wrong prev_term at index 11
    public void testAppendWithConflictingTerm() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();

        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 1);

        // now append(index=11,term=5) -> should return false result with index=8
        AppendResult result=append(impl, 11, 7, new LogEntry(6, buf), leader, 1);
        assert !result.success();
        assertEquals(result.index(), 8);
        assertEquals(result.nonMatchingTerm(), 6);
        assertLogIndices(log, 10, 1, 6);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    public void testRAFTPaperAppendOnLeader() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 10);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 1);
        assertTrue(result.isSuccess());
        assertEquals(result.getIndex(), 11);
        assertLogIndices(log, 11, 10, 6);
    }

    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06    06 <-- add
    public void testRAFTPaperScenarioA() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 9);
        AppendResult result = append(impl, 11, 6, new LogEntry(6, buf), leader, 9);
        assertFalse(result.isSuccess());
        assertEquals(result.getIndex(), 9);
        assertLogIndices(log, 9, 9, 6);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04                   06 <-- add
    public void testRAFTPaperScenarioB() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl, 2, 1, new LogEntry(1, buf), leader, 1);
        append(impl, 3, 1, new LogEntry(1, buf), leader, 1);
        append(impl, 4, 1, new LogEntry(4, buf), leader, 4);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 4);
        assertFalse(result.isSuccess());
        assertEquals(result.getIndex(), 4);
        assertLogIndices(log, 4, 4, 4);
    }

    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06 06 06
    public void testRAFTPaperScenarioC() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 11, 6, new LogEntry(6, buf), leader, 11);
        // Overwrites existing entry; does *not* advance last_applied in log
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 11);
        assertTrue(result.isSuccess());
        assertEquals(result.getIndex(), 11);
        assertLogIndices(log, 11, 11, 6);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06 06 07 07
    public void testRAFTPaperScenarioD() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 11, 6, new LogEntry(7, buf), leader, 1);
        append(impl, 12, 7, new LogEntry(7, buf), leader, 12);

        AppendResult result=append(impl, buf, leader, 10, 6, 8, 12);
        assertTrue(result.isSuccess());
        assertEquals(result.getIndex(), 11);
        assertLogIndices(log, 11, 11, 8);
    }

    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 04 04
    public void testRAFTPaperScenarioE() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  7, 4, new LogEntry(4, buf), leader, 7);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 7);
        assertFalse(result.isSuccess());
        assertEquals(result.getIndex(), 7);
        assertLogIndices(log, 7, 7, 4);
    }

    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 02 02 02 03 03 03 03 03
    public void testRAFTPaperScenarioF() throws Exception {
        Address leader=Util.createRandomAddress("A");
        initB();
        RaftImpl impl=getImpl(b);
        Log log=impl.raft().log();
        byte[] buf=new byte[10];
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(2, buf), leader, 1);
        append(impl,  5, 2, new LogEntry(2, buf), leader, 1);
        append(impl,  6, 2, new LogEntry(2, buf), leader, 1);
        append(impl,  7, 2, new LogEntry(3, buf), leader, 1);
        append(impl,  8, 3, new LogEntry(3, buf), leader, 1);
        append(impl,  9, 3, new LogEntry(3, buf), leader, 1);
        append(impl, 10, 3, new LogEntry(3, buf), leader, 1);
        append(impl, 11, 3, new LogEntry(3, buf), leader, 11);

        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 11);
        assertFalse(result.isSuccess());
        assertEquals(result.getIndex(), 7);
        assertLogIndices(log, 11, 11, 3);
    }

    protected static JChannel create(String name, boolean follower) throws Exception {
        return create(name, follower, r -> r);
    }

    protected static JChannel create(String name, boolean follower, Function<RAFT, RAFT> config) throws Exception {
        return create(name, follower, members, config);
    }

    protected static JChannel create(String name, boolean follower, final List<String> members) throws Exception {
        return create(name, follower, members, r -> r);
    }

    protected static JChannel create(String name, boolean follower, final List<String> members, Function<RAFT, RAFT> config) throws Exception {
        ELECTION election=new ELECTION().noElections(follower);
        RAFT raft=config.apply(new RAFT()).members(members).raftId(name)
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logName(name + "-" + CLUSTER);
        return new JChannel(Util.getTestStack(election, raft, new REDIRECT())).name(name);
    }


    protected static void close(boolean remove_log, boolean remove_snapshot, JChannel... channels) {
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            if(remove_log)
                raft.log().delete(); // remove log files after the run
            if(remove_snapshot)
                raft.deleteSnapshot();
            Util.close(ch);
        }
    }

    protected void init(boolean verbose) throws Exception {
        init(verbose, r -> r);
    }

    protected void init(boolean verbose, Function<RAFT, RAFT> config) throws Exception {
        c=create("C", true, config);  // follower
        cs=new ReplicatedStateMachine<>(c);
        b=create("B", true, config);  // follower
        bs=new ReplicatedStateMachine<>(b);
        a=create("A", false, config); // leader
        as=new ReplicatedStateMachine<>(a);
        c.connect(CLUSTER);
        b.connect(CLUSTER);
        a.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);

        for(int i=0; i < 20; i++) {
            if(isLeader(a) && !isLeader(b) && !isLeader(c))
                break;
            Util.sleep(500);
        }
        if(verbose) {
            System.out.println("A: is leader? -> " + isLeader(a));
            System.out.println("B: is leader? -> " + isLeader(b));
            System.out.println("C: is leader? -> " + isLeader(c));
        }
        assert isLeader(a);
        assert !isLeader(b);
        assert !isLeader(c);
    }

    protected void initB() throws Exception {
        b=create("B", true); // follower
        raft(b).stateMachine(new DummyStateMachine());
        b.connect(CLUSTER);
    }

    protected static boolean isLeader(JChannel ch) {
        RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
        return ch.getAddress().equals(raft.leader());
    }

    protected static RaftImpl getImpl(JChannel ch) {
        RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
        Field impl=Util.getField(RAFT.class, "impl");
        return (RaftImpl)Util.getField(impl, raft);
    }

    protected static void assertLeader(JChannel ch, long timeout, long interval) {
        RAFT raft=raft(ch);
        long stop_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < stop_time) {
            if(raft.isLeader())
                break;
            Util.sleep(interval);
        }
        assert raft.isLeader();
    }

    protected void assertPresent(int key, int value, ReplicatedStateMachine<Integer,Integer> ... rsms) {
        if(rsms == null || rsms.length == 0)
            rsms=new ReplicatedStateMachine[]{as,bs,cs};
        for(int i=0; i < 10; i++) {
            boolean found=true;
            for(ReplicatedStateMachine<Integer,Integer> rsm: rsms) {
                Integer val=rsm.get(key);
                if(!Objects.equals(val, value)) {
                    found=false;
                    break;
                }
            }
            if(found)
                break;
            Util.sleep(500);
        }

        for(ReplicatedStateMachine<Integer,Integer> rsm: rsms) {
            Integer val=rsm.get(key);
            assert Objects.equals(val, value);
            System.out.println("rsm = " + rsm);
        }
    }

    @SafeVarargs
    protected final void assertSame(ReplicatedStateMachine<Integer,Integer>... rsms) {
        ReplicatedStateMachine<Integer,Integer> first=rsms[0];
        for(int i=0; i < 10; i++) {
            boolean same=true;
            for(int j=1; j < rsms.length; j++) {
                ReplicatedStateMachine<Integer,Integer> rsm=rsms[j];
                if(!rsm.equals(first)) {
                    same=false;
                    break;
                }
            }
            if(same)
                break;
            Util.sleep(500);
        }
        for(ReplicatedStateMachine<Integer,Integer> rsm: rsms)
            System.out.println(rsm.channel().getName() + ": " + rsm);

        for(int j=1; j < rsms.length; j++) {
            ReplicatedStateMachine<Integer,Integer> rsm=rsms[j];
            assert rsm.equals(first) : String.format("commit-table of A: %s",
                                                     ((RAFT)a.getProtocolStack().findProtocol(RAFT.class)).dumpCommitTable());
        }
    }

    protected static void assertLogIndices(Log log, int last_applied, int commit_index, int term) {
        assertEquals(log.lastAppended(), last_applied);
        assertEquals(log.commitIndex(), commit_index);
        assertEquals(log.currentTerm(), term);
    }

    protected static void assertCommitIndex(long timeout, long interval, int expected_commit, int expected_applied, JChannel... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                RAFT raft=raft(ch);
                if(expected_commit != raft.commitIndex() || expected_applied != raft.lastAppended())
                    all_ok=false;
            }
            if(all_ok)
                break;
            Util.sleep(interval);
        }
        for(JChannel ch: channels) {
            RAFT raft=raft(ch);
            System.out.printf("%s: last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
            assert raft.commitIndex() == expected_commit && raft.lastAppended() == expected_applied
              : String.format("%s: last-applied=%d, commit-index=%d", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
        }
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }

    protected static AppendResult append(RaftImpl impl, int index, int prev_term, LogEntry entry, Address leader, int leader_commit) throws Exception {
        return append(impl, entry.command(), leader, Math.max(0, index-1), prev_term, entry.term(), leader_commit);
    }

    protected static AppendResult append(RaftImpl impl, byte[] data, Address leader,
                                         int prev_log_index, int prev_log_term, int entry_term, int leader_commit) throws Exception {
        return (AppendResult)handleAppendEntriesRequest.invoke(impl, data, 0, data.length, leader,
                                                               prev_log_index, prev_log_term, entry_term, leader_commit, false);
    }


    protected static class DummyStateMachine implements StateMachine {
        public byte[] apply(byte[] data, int offset, int length) throws Exception {return new byte[0];}
        public void readContentFrom(DataInput in) throws Exception {}
        public void writeContentTo(DataOutput out) throws Exception {}
    }
}
