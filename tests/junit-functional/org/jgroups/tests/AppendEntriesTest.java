package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.raft.ReplicatedStateMachine;
import org.jgroups.protocols.raft.CLIENT;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

/**
 * Tests the AppendEntries functionality: appending log entries in regular operation, new members, late joiners etc
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class AppendEntriesTest {
    protected JChannel                                a,  b,  c;  // A is always the leader
    protected ReplicatedStateMachine<Integer,Integer> as, bs, cs;
    protected static final String CLUSTER="AppendEntriesTest";
    protected static final int    MAJORITY=2;

    @AfterMethod
    protected void destroy() {
        close(true, true, c,b,a);
    }


    public void testNormalOperation() throws Exception {
        init(true);
        for(int i=1; i <= 10; i++)
            as.put(i, i);
        assertSame(as, bs, cs);
        bs.remove(5);
        cs.put(11, 11);
        cs.remove(1);
        as.put(1,1);
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
     * state machine will time out and the replicated state machine will be empty.
     */
    public void testNonCommitWithoutMajority() throws Exception {
        init(true);
        Util.close(b,c);
        as.timeout(500);

        for(int i=1; i <= 3; i++) {
            try {
                as.put(i, i);
            }
            catch(TimeoutException ex) {
                System.out.println("received " + ex.getClass().getSimpleName() + " as expected; cache size is " + as.size());
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

        // Now start C again
        c=create("C", true);  // follower
        cs=new ReplicatedStateMachine<>(c);
        c.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a,b,c);

        // Now C should also have the same entries (1-5) as A and B
        assertSame(as, bs, cs);
    }


    /**
     * Leader A and follower B commit 5 entries. Then C comes up and should get the 5 committed entries as well,
     * as a snapshot
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
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a,b,c);

        assertSame(as, bs, cs);
    }






    protected JChannel create(String name, boolean follower) throws Exception {
        ELECTION election=new ELECTION().noElections(follower);
        RAFT raft=new RAFT().majority(MAJORITY).logClass("org.jgroups.protocols.raft.InMemoryLog").logName(name);
        // RAFT raft=new RAFT().majority(MAJORITY).logName(name);
        CLIENT client=new CLIENT();
        return new JChannel(Util.getTestStack(election, raft, client)).name(name);
    }


    protected void close(boolean remove_log, boolean remove_snapshot, JChannel ... channels) {
        for(JChannel ch: channels) {
            RAFT raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
            if(remove_log)
                raft.log().delete(); // remove log files after the run
            if(remove_snapshot)
                raft.deleteSnapshot();
            Util.close(ch);
        }
    }

    protected void init(boolean verbose) throws Exception {
        c=create("C", true);  // follower
        cs=new ReplicatedStateMachine<>(c);
        b=create("B", true);  // follower
        bs=new ReplicatedStateMachine<>(b);
        a=create("A", false); // leader
        as=new ReplicatedStateMachine<>(a);
        c.connect(CLUSTER);
        b.connect(CLUSTER);
        a.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a,b,c);

        for(int i=0; i < 10; i++) {
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

    protected boolean isLeader(JChannel ch) {
        RAFT raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
        return ch.getAddress().equals(raft.leader());
    }

    protected void assertPresent(int key, int value, ReplicatedStateMachine<Integer,Integer> ... rsms) {
        if(rsms == null || rsms.length == 0)
            rsms=new ReplicatedStateMachine[]{as,bs,cs};
        for(int i=0; i < 10; i++) {
            boolean found=true;
            for(ReplicatedStateMachine<Integer,Integer> rsm: rsms) {
                Integer val=rsm.get(key);
                if(val == null || !val.equals(value)) {
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
            assert val != null && val.equals(value);
            System.out.println("rsm = " + rsm);
        }
    }

    protected void assertSame(ReplicatedStateMachine<Integer,Integer> ... rsms) {
        if(rsms == null || rsms.length == 0)
            rsms=new ReplicatedStateMachine[]{as, bs, cs};
        if(rsms.length < 2)
            return;
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
            assert rsm.equals(first);
        }
    }

}


// TODO: turn the tests below into unit tests
/*
   public void testRAFTPaperAppendOnLeader(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06

        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(4, buf));
        log.append(5, false, new LogEntry(4, buf));
        log.append(6, false, new LogEntry(5, buf));
        log.append(7, false, new LogEntry(5, buf));
        log.append(8, false, new LogEntry(6, buf));
        log.append(9, false, new LogEntry(6, buf));
        log.append(10, false, new LogEntry(6, buf));
        log.commitIndex(10);
        AppendResult result = log.append(10, 6, new LogEntry(6, buf));
        assertTrue(result.isSuccess());
        assertEquals(result.getIndex(), 11);
    }

    public void testRAFTPaperScenarioA(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06
        // Flwr A 01 01 01 04 04 05 05 06 06

        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(4, buf));
        log.append(5, false, new LogEntry(4, buf));
        log.append(6, false, new LogEntry(5, buf));
        log.append(7, false, new LogEntry(5, buf));
        log.append(8, false, new LogEntry(6, buf));
        log.append(9, false, new LogEntry(6, buf));
        log.commitIndex(9);
        AppendResult result = log.append(10, 6, new LogEntry(6, buf));
        assertFalse(result.isSuccess());
        //assertEquals(result.getIndex(), 9, 6);
    }

    public void testRAFTPaperScenarioB(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06
        // Flwr A 01 01 01 04

        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(4, buf));
        log.commitIndex(4);
        AppendResult result = log.append(10, 6, new LogEntry(6, buf));
        assertFalse(result.isSuccess());
        //assertEquals(result.getIndex(), -1, 6);
    }

    public void testRAFTPaperScenarioC(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06
        // Flwr A 01 01 01 04 04 05 05 06 06 06 06

        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(4, buf));
        log.append(5, false, new LogEntry(4, buf));
        log.append(6, false, new LogEntry(5, buf));
        log.append(7, false, new LogEntry(5, buf));
        log.append(8, false, new LogEntry(6, buf));
        log.append(9, false, new LogEntry(6, buf));
        log.append(10, false, new LogEntry(6, buf));
        log.append(11, false, new LogEntry(6, buf));
        log.commitIndex(11);
        AppendResult result = log.append(10, 6, new LogEntry(6, buf));
        assertTrue(result.isSuccess());
        assertEquals(result.getIndex(), 11);
    }

    public void testRAFTPaperScenarioD(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06
        // Flwr A 01 01 01 04 04 05 05 06 06 06 07 07

        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(4, buf));
        log.append(5, false, new LogEntry(4, buf));
        log.append(6, false, new LogEntry(5, buf));
        log.append(7, false, new LogEntry(5, buf));
        log.append(8, false, new LogEntry(6, buf));
        log.append(9, false, new LogEntry(6, buf));
        log.append(10, false, new LogEntry(6, buf));
        log.append(11, false, new LogEntry(7, buf));
        log.append(12, false, new LogEntry(7, buf));
        log.commitIndex(12);
        AppendResult result = log.append(10, 6, new LogEntry(6, buf));
        assertTrue(result.isSuccess());
        assertEquals(result.getIndex(), 11);
    }

    public void testRAFTPaperScenarioE(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06
        // Flwr A 01 01 01 04 04 04 04

        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(4, buf));
        log.append(5, false, new LogEntry(4, buf));
        log.append(6, false, new LogEntry(4, buf));
        log.append(7, false, new LogEntry(4, buf));
        log.commitIndex(7);
        AppendResult result = log.append(10, 6, new LogEntry(6, buf));
        assertFalse(result.isSuccess());
        //assertEquals(result.getIndex(), -1, 6);
    }

    public void testRAFTPaperScenarioF(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06
        // Flwr A 01 01 01 02 02 02 03 03 03 03 03

        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(2, buf));
        log.append(5, false, new LogEntry(2, buf));
        log.append(6, false, new LogEntry(2, buf));
        log.append(7, false, new LogEntry(3, buf));
        log.append(8, false, new LogEntry(3, buf));
        log.append(9, false, new LogEntry(3, buf));
        log.append(10, false, new LogEntry(3, buf));
        log.append(11, false, new LogEntry(3, buf));
        log.commitIndex(11);
        AppendResult result = log.append(10, 6, new LogEntry(6, buf));
        assertFalse(result.isSuccess());
        //assertEquals(result.getIndex(), -1, 6);
    }
*/