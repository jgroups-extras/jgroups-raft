package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

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
          {new LevelDBLog()},
          {new InMemoryLog()}
        };
    }

    @AfterMethod protected void destroy() {
        if(log != null) {
            log.delete();
            log=null;
        }
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

        log.close();
        log.delete();
        log.init(filename, null);
        current_term=log.currentTerm();
        assertEquals(current_term, 0);
        voted_for=log.votedFor();
        assertNull(voted_for);
    }


    public void testNewLog(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        assertIndices(0, 0, 0, 0);
        assertNull(log.votedFor());
    }

    public void testNewLogAfterDelete(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        append(log, 1, false, new byte[10], 5,5,5);
        log.commitIndex(2);
        assertIndices(0, 3, 2, 5);
        log.close();
        log.delete();
        log.init(filename, null);
        assertIndices(0,0,0,0);
        assertNull(log.votedFor());
    }

    public void testMetadataInAReopenedLog(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1,1,1, 4,4, 5,5, 6,6,6);
        log.commitIndex(10);
        log.votedFor(Util.createRandomAddress("A"));
        log.close();
        log.init(filename, null);
        assertIndices(0, 10, 10, 6);
        assertEquals(log.votedFor().toString(), Util.createRandomAddress("A").toString());
    }

    public void testAppendOnLeader(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, false, new LogEntry(5, buf));
        log.append(2, false, new LogEntry(5, buf));
        assertIndices(0, 2, 0, 5);
    }

    public void testAppendMultipleEntries(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        LogEntry[] entries={new LogEntry(1, buf), new LogEntry(1, buf), new LogEntry(3, buf)};
        log.append(1, false, entries);
        assertIndices(0,3,0,3);

        entries=new LogEntry[30];
        for(int i=0; i < entries.length; i++)
            entries[i]=new LogEntry(Math.max(3,i/2), buf);
        log.append(4, false, entries);
        assertIndices(0, 33, 0, 29/2);
    }

    public void testDeleteEntriesInTheMiddle(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1,1,1, 2,2,2, 3,3,3,3,3);
        log.commitIndex(11);

        log.deleteAllEntriesStartingFrom(6);
        assertIndices(0,5,5,2);

        for(int i=1; i <= 5; i++)
            assertNotNull(log.get(i));
        for(int i=6; i <= 11; i++)
            assertNull(log.get(i));

    }

    public void testDeleteEntriesFromFirst(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1,1,1, 2,2,2, 3,3,3,3,3);
        log.commitIndex(11);

        log.deleteAllEntriesStartingFrom(1);
        assertIndices(0,0,0,0);
        for(int i=1; i <= 11; i++)
            assertNull(log.get(i));
    }

    public void testDeleteEntriesFromLast(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        log.commitIndex(11);

        log.deleteAllEntriesStartingFrom(11);
        assertIndices(0, 10, 10, 3);
        for(int i=1; i <= 10; i++)
            assertNotNull(log.get(i));
        assertNull((log.get(11)));
    }

    public void testTruncateInTheMiddle(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        log.commitIndex(11);

        log.truncate(6);
        assertIndices(6, 11, 11, 3);
        for(int i=1; i <= 5; i++)
            assertNull(log.get(i));
        for(int i=6; i <= 11; i++)
            assertNotNull(log.get(i));
    }

    public void testTruncateFirst(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        log.commitIndex(11);

        log.truncate(1);
        assertIndices(1, 11, 11, 3);
        assertNotNull(log.get(1));
    }

    public void testTruncateLast(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        log.commitIndex(11);

        log.truncate(11);
        assertIndices(11, 11, 11, 3);
        for(int i=1; i <= 10; i++)
            assertNull(log.get(i));
        assertNotNull(log.get(11));
    }


    public void testTruncateAndReopen(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, false, buf, 1, 1, 1, 2, 2);
        log.commitIndex(5);

        log.truncate(4);
        log.close();
        log.init(filename, null);
        assertEquals(log.firstApplied(), 4);
        assertEquals(log.lastApplied(), 5);
        for(int i=1; i <= 3; i++)
            assertNull(log.get(i));
        for(int i=4; i <= 5; i++)
            assertNotNull(log.get(i));
    }


    public void testTruncateTwice(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        for(int i=1; i <= 10; i++)
            log.append(i, false, new LogEntry(5, buf));
        log.commitIndex(6);
        log.truncate(4);
        assertEquals(log.commitIndex(), 6);
        assertEquals(log.firstApplied(), 4);

        log.commitIndex(10);
        log.truncate(8);
        assertEquals(log.commitIndex(), 10);
        assertEquals(log.firstApplied(), 8);
    }


    public void testForEach(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        for(int i=1; i <= 10; i++)
            log.append(i, false, new LogEntry(5, buf));
        log.commitIndex(8);

        final AtomicInteger cnt=new AtomicInteger(0);
        Log.Function func=new Log.Function() {
            @Override
            public boolean apply(int index, int term, byte[] command, int offset, int length, boolean internal) {
                cnt.incrementAndGet();
                return true;
            }
        };

        log.forEach(func);
        assert cnt.get() == 10;

        cnt.set(0);
        log.truncate(8);

        append(log, 11, false, buf, 6,6,6, 7,7,7, 8,8,8,8);
        log.forEach(func);
        assert cnt.get() == 13;

        cnt.set(0);
        log.forEach(func, 0, 25);
        assert cnt.get() == 13;
    }



    protected void append(final Log log, int start_index, boolean overwrite, final byte[] buf, int ... terms) {
        int index=start_index;
        for(int term: terms) {
            log.append(index, overwrite, new LogEntry(term, buf));
            index++;
        }
    }

    protected void assertIndices(int first_applied, int last_applied, int commit_index, int current_term) {
        assertEquals(log.firstApplied(), first_applied);
        assertEquals(log.lastApplied(),  last_applied);
        assertEquals(log.commitIndex(),  commit_index);
        assertEquals(log.currentTerm(),  current_term);
    }

}
