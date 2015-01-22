package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.raft.ReplicatedStateMachine;
import org.jgroups.protocols.raft.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Tests the AppendEntries functionality: appending log entries in regular operation, new members, late joiners etc
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class AppendEntriesTest {
    protected JChannel                                a,  b,  c;  // A is always the leader
    protected ReplicatedStateMachine<Integer,Integer> as, bs, cs;

    @AfterMethod
    protected void destroy() {
        for(JChannel ch: Arrays.asList(c,b,a)) {
            if(ch == null)
                continue;
            RAFT raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
            raft.log().delete(); // remove log files after the run
        }
        Util.close(c,b,a);
    }




    protected JChannel create(String name, boolean follower) throws Exception {
        ELECTION election=new ELECTION().noElections(follower);
        RAFT raft=new RAFT().majority(2);
        CLIENT client=new CLIENT();
        return new JChannel(Util.getTestStack(election, raft, client)).name(name);
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