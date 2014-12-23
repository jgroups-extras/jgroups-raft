package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Tests all {@link org.jgroups.protocols.raft.Log} implementations for correctness
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="logProvider")
public class LogTest {
    protected Log log;
    protected static final String filename="raft.log";


    @DataProvider static Object[][] logProvider() {
        return new Object[][] {
          /*{new MapDBLog()},*/
          {new LevelDBLog()}
        };
    }

    @AfterMethod protected void destroy() {
        if(log != null) {
            log.delete();
        }
        log=null;
    }



    public void testFields(Log log) throws Exception {
        Address addr=Util.createRandomAddress("A");
        this.log=log;
        log.init(filename, null);
        log.currentTerm(22);
        int current_term=log.currentTerm();
        assertEquals(current_term, 22);

        log.votedFor(addr);
        Address voted_for=log.votedFor();
        assertEquals(addr, voted_for);

        log.close();
        log.init(filename, null);
        current_term=log.currentTerm();
        assertEquals(current_term, 22);
        voted_for=log.votedFor();
        assertEquals(addr, voted_for);

        /*
        log.close();
        log.delete();
        log.init(filename, null);
        current_term=log.currentTerm();
        assertEquals(current_term, 0);
        voted_for=log.votedFor();
        assertNull(voted_for);
        */
    }

    public void testNewLog(Log log) throws Exception {

        this.log=log;
        log.init(filename, null);

        assertEquals(log.firstApplied(), -1);
        assertEquals(log.lastApplied(), 0);
        assertEquals(log.currentTerm(), 0);
        assertEquals(log.commitIndex(), 0);
        assertNull(log.votedFor());

    }

    public void testAppendOnLeader(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(5, buf));
        log.append(2, false, new LogEntry(5, buf));
        assertEquals(log.lastApplied(), 2);
        assertEquals(log.commitIndex(), 0);
        assertEquals(log.firstApplied(), 1);
    }

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

    public void testDeleteEntriesInTheMiddle(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06

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

        log.deleteAllEntriesStartingFrom(6);

        assertEquals(log.firstApplied(), 1);
        assertEquals(log.lastApplied(), 5);
        assertEquals(log.currentTerm(), 2);
        //assertEquals(log.commitIndex(), ??);

    }

    public void testDeleteEntriesFromFirst(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06

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

        log.deleteAllEntriesStartingFrom(1);

        assertEquals(log.firstApplied(), 1);
        assertEquals(log.lastApplied(), 0);
        assertEquals(log.currentTerm(), 0);
        //assertEquals(log.commitIndex(), ??);

    }

    public void testDeleteEntriesFromLast(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06

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

        log.deleteAllEntriesStartingFrom(11);

        assertEquals(log.lastApplied(), 10);
        assertEquals(log.currentTerm(), 3);
        //assertEquals(log.commitIndex(), ??);

    }

    public void testTruncateInTheMiddle(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06

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

        log.truncate(6);

        assertEquals(log.lastApplied(), 11);
        assertEquals(log.currentTerm(), 3);
        assertEquals(log.firstApplied(), 6);
        //assertEquals(log.commitIndex(), ??);

    }

    public void testTruncateFirst(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06

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

        log.truncate(1);

        assertEquals(log.lastApplied(), 11);
        assertEquals(log.currentTerm(), 3);
        assertEquals(log.firstApplied(), 1);
        //assertEquals(log.commitIndex(), ??);

    }

    public void testTruncateLast(Log log) throws Exception {
        // Index  01 02 03 04 05 06 07 08 09 10 11 12
        // Leader 01 01 01 04 04 05 05 06 06 06

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

        log.truncate(11);

        assertEquals(log.lastApplied(), 11);
        assertEquals(log.currentTerm(), 3);
        assertEquals(log.firstApplied(), 11);
        //assertEquals(log.commitIndex(), ??);

    }


    public void testTruncateAndReopen(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(1, buf));
        log.append(2, false, new LogEntry(1, buf));
        log.append(3, false, new LogEntry(1, buf));
        log.append(4, false, new LogEntry(2, buf));
        log.append(5, false, new LogEntry(2, buf));
        log.truncate(4);

        log.close();
        log.init(filename, null);
        assertEquals(log.firstApplied(), 4);
        assertEquals(log.lastApplied(),5);
        for(int i=1; i <= 3; i++)
            assertNull(log.get(i));
    }



    /*
    public void testIterator(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, new LogEntry(5, buf));
        log.append(2, new LogEntry(5, buf));
        log.forEach(null);
        assertTrue(false);
    }
    */
}
