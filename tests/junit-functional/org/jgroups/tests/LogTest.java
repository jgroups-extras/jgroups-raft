package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.*;
import org.jgroups.raft.util.LogCache;
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
    protected static final String filename="LogTest.log";


    @DataProvider static Object[][] logProvider() {
        return new Object[][] {
          {new LevelDBLog().useFsync(true)},
          {new InMemoryLog().useFsync(true)},
          {new FileBasedLog().useFsync(true)},
          {new LogCache(new LevelDBLog().useFsync(true), 512)}
        };
    }

    @AfterMethod protected void destroy() throws Exception {
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
        long current_term=log.currentTerm();
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
        append(log, 1, new byte[10], 5,5,5);
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
        append(log, 1, buf, 1,1,1, 4,4, 5,5, 6,6,6);
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
        LogEntries entries=LogEntries.create(new LogEntry(5, buf), new LogEntry(5, buf));
        log.append(1, entries);
        assertIndices(0, 2, 0, 5);
    }

    public void testAppendMultipleEntries(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        LogEntries le=new LogEntries().add(new LogEntry(1, buf), new LogEntry(1, buf), new LogEntry(3, buf));
        log.append(1, le);
        assertIndices(0,3,0,3);

        le.clear();
        for(int i=0; i < 30; i++)
            le.add(new LogEntry(Math.max(3,i/2), buf));
        log.append(4, le);
        assertIndices(0, 33, 0, 29/2);
    }

    public void testAppendMultipleEntriesOneByOne(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        LogEntries le=new LogEntries();
        for(int i=0; i < 30; i++)
            le.add(new LogEntry(i+1, buf));
        log.append(1, le);
        assertIndices(0,30,0,30);
    }

    public void testDeleteEntriesInTheMiddle(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, buf, 1,1,1, 2,2,2, 3,3,3,3,3);
        log.commitIndex(5);

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
        append(log, 1,  buf, 1,1,1, 2,2,2, 3,3,3,3,3);
        // RAFT does not allow rollback
        // log.commitIndex(11);

        log.deleteAllEntriesStartingFrom(1);
        assertIndices(0,0,0,0);
        for(int i=1; i <= 11; i++)
            assertNull(log.get(i));
    }

    public void testDeleteEntriesFromLast(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        log.commitIndex(10);

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
        append(log, 1, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
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
        append(log, 1, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        log.commitIndex(11);

        log.truncate(1);
        assertIndices(1, 11, 11, 3);
        assertNotNull(log.get(1));
    }

    public void testTruncateLast(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
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
        append(log, 1, buf, 1, 1, 1, 2, 2);
        log.commitIndex(5);

        log.truncate(4);
        log.close();
        log.init(filename, null);
        assertEquals(log.firstAppended(), 4);
        assertEquals(log.lastAppended(), 5);
        for(int i=1; i <= 3; i++)
            assertNull(log.get(i));
        for(int i=4; i <= 5; i++)
            assertNotNull(log.get(i));
    }


    public void testTruncateAtCommitIndexAndReopen(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, buf, 1, 1, 1, 2, 2,2,2,3,4,5);
        long last_appended=log.lastAppended();
        log.commitIndex(last_appended);
        log.truncate(log.commitIndex());
        log.close();
        log.init(filename, null);
        assert log.firstAppended() == 10;
        assert log.lastAppended() == 10;
        assert log.commitIndex() == 10;
        for(int i=1; i < 10; i++)
            assert log.get(i) == null;
        assert log.get(10) != null;
    }

    public void testTruncateTwice(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        LogEntries le=new LogEntries();
        for(int i=1; i <= 10; i++)
            le.add(new LogEntry(5, buf));
        log.append(1, le);
        log.commitIndex(6);
        log.truncate(4);
        assertEquals(log.commitIndex(), 6);
        assertEquals(log.firstAppended(), 4);

        log.commitIndex(10);
        log.truncate(8);
        assertEquals(log.commitIndex(), 10);
        assertEquals(log.firstAppended(), 8);
    }

    public void testTruncateOnlyCommitted(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        LogEntries le=new LogEntries();
        for(int i=1; i <= 10; i++)
            le.add(new LogEntry(5, buf));
        log.append(1, le);

        log.commitIndex(5);
        log.truncate(10);
        assertEquals(log.commitIndex(), 5);
        assertEquals(log.firstAppended(), 5);
    }

    public void testReinitializeTo(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        LogEntries le=new LogEntries();
        for(int i=1; i <= 10; i++)
            le.add(new LogEntry(5, buf));
        log.append(1, le);
        assertIndices(0, 10, 0, 5);
        log.reinitializeTo(5, new LogEntry(10, null));
        assertIndices(5, 5, 5, 10);
        assert log.size() == 1;
        long last=log.append(6, LogEntries.create(new LogEntry(15, null)));
        assert last == 6;
        assert log.size() == 2;
        assertIndices(5,6,5,15);
    }


    public void testForEach(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        LogEntries le=new LogEntries();
        for(int i=1; i <= 10; i++)
            le.add(new LogEntry(i, buf));
        log.append(1, le);
        log.commitIndex(8);

        final AtomicInteger cnt=new AtomicInteger(0);
        log.forEach((entry,index) -> cnt.incrementAndGet());
        assertEquals(cnt.get(), 10);

        cnt.set(0);
        log.truncate(8);

        append(log, 11, buf, 6,6,6, 7,7,7, 8,8,8,8);
        log.forEach((entry,index) -> cnt.incrementAndGet());
        assertEquals(cnt.get(), 13);

        cnt.set(0);
        log.forEach((entry,index) -> cnt.incrementAndGet(), 0, 25);
        assertEquals(cnt.get(), 13);
    }

    public void testSize(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        assert log.size() == 0;
        LogEntries le=new LogEntries();
        le.add(new LogEntry(5, buf));
        log.append(1, le);

        assert log.size() == 1;
        le.clear().add(new LogEntry(5, buf));
        log.append(2, le);
        assert log.size() == 2;

        le.clear();
        for(int i=3; i <= 8; i++)
            le.add(new LogEntry(5, buf));
        log.append(3, le);
        assert log.size() == 8;
        log.commitIndex(3);
        log.truncate(3); // excluding 3
        assert log.size() == 6;
    }

    public void testSizeInBytes(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        assert log.sizeInBytes() == 0;
        byte[] buf=new byte[50];
        final int NUM=100;
        LogEntries le=new LogEntries();
        for(int i=0; i < NUM; i++)
            le.add(new LogEntry(1, buf));
        long last=log.append(1, le);
        assert last == NUM;
        long size_in_bytes=log.sizeInBytes();
        assert size_in_bytes >= buf.length * NUM;
    }

    public void testInternalCommand(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        assert log.sizeInBytes() == 0;
        byte[] buf=new byte[10];
        LogEntries entries = new LogEntries();

        for (int i = 1; i<=10; i++) {
            LogEntry entry = new LogEntry(i, buf);
            entries.add(entry.internal((i & 1) == 0));
        }

        long last = log.append(1, entries);
        assert last == entries.size() : "Not stored all entries";

        log.forEach((e, l) -> {
            assert e.internal() == ((l & 1) == 0) : "Entry not internal anymore!";
        });
    }


    protected static void append(final Log log, int start_index, final byte[] buf, int... terms) {
        LogEntries le=new LogEntries();
        for(int term: terms)
            le.add(new LogEntry(term, buf));
        log.append(start_index, le);
    }

    protected void assertIndices(int first_applied, int last_applied, int commit_index, int current_term) {
        assertEquals(log.firstAppended(), first_applied, "Incorrect first appended");
        assertEquals(log.lastAppended(),  last_applied, "Incorrect last appended");
        assertEquals(log.commitIndex(),  commit_index, "Incorrect commit index");
        assertEquals(log.currentTerm(),  current_term, "Incorrect current term");
    }

}
