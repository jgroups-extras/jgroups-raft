package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.raft.util.LogCache;
import org.jgroups.util.Util;

import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
          {new InMemoryLog().useFsync(true)},
          {new FileBasedLog().useFsync(true)},
          // TODO The LogCache's constructor calls methods on the underlying log without calling init(..) first,
          // which won't work with Log impls that expect proper lifecycle contract.
          {new LogCache(new InMemoryLog().useFsync(true), 512)}
        };
    }

    @AfterMethod protected void destroy() throws Exception {
        if(log != null) {
            log.close();
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
        assertThat(current_term).isEqualTo(22);

        log.votedFor(addr);
        Address voted_for=log.votedFor();
        assertThat(voted_for).isEqualTo(addr);

        log.close();
        log.init(filename, null);
        current_term=log.currentTerm();
        assertThat(current_term).isEqualTo(22);
        voted_for=log.votedFor();
        assertThat(voted_for).isEqualTo(addr);

        log.close();
        log.delete();
        log.init(filename, null);
        current_term=log.currentTerm();
        assertThat(current_term).isEqualTo(0);
        voted_for=log.votedFor();
        assertThat(voted_for).isNull();
    }


    public void testNewLog(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        assertIndices(0, 0, 0, 0);
        assertThat(log.votedFor()).isNull();
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
        assertThat(log.votedFor()).isNull();
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
        assertThat(log.votedFor().toString()).isEqualTo(Util.createRandomAddress("A").toString());
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
            assertThat(log.get(i)).isNotNull();
        for(int i=6; i <= 11; i++)
            assertThat(log.get(i)).isNull();

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
            assertThat(log.get(i)).isNull();
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
            assertThat(log.get(i)).isNotNull();
        assertThat(log.get(11)).isNull();
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
            assertThat(log.get(i)).isNull();
        for(int i=6; i <= 11; i++)
            assertThat(log.get(i)).isNotNull();
    }

    public void testTruncateFirst(Log log) throws Exception {
        this.log = log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        append(log, 1, buf, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
        log.commitIndex(11);

        log.truncate(1);
        assertIndices(1, 11, 11, 3);
        assertThat(log.get(1)).isNotNull();
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
            assertThat(log.get(i)).isNull();
        assertThat(log.get(11)).isNotNull();
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
        assertThat(log.firstAppended()).isEqualTo(4);
        assertThat(log.lastAppended()).isEqualTo(5);
        for(int i=1; i <= 3; i++)
            assertThat(log.get(i)).isNull();
        for(int i=4; i <= 5; i++)
            assertThat(log.get(i)).isNotNull();
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
        assertThat(log.firstAppended()).isEqualTo(10);
        assertThat(log.lastAppended()).isEqualTo(10);
        assertThat(log.commitIndex()).isEqualTo(10);
        SoftAssertions.assertSoftly(softly -> {
            for(int i=1; i < 10; i++)
                softly.assertThat(log.get(i)).isNull();
        });
        assertThat(log.get(10)).isNotNull();
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
        assertThat(log.commitIndex()).isEqualTo(6);
        assertThat(log.firstAppended()).isEqualTo(4);

        log.commitIndex(10);
        log.truncate(8);
        assertThat(log.commitIndex()).isEqualTo(10);
        assertThat(log.firstAppended()).isEqualTo(8);
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
        assertThat(log.commitIndex()).isEqualTo(5);
        assertThat(log.firstAppended()).isEqualTo(5);
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
        assertThat(log.size()).isEqualTo(1);
        long last=log.append(6, LogEntries.create(new LogEntry(15, null)));
        assertThat(last).isEqualTo(6);
        assertThat(log.size()).isEqualTo(2);
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
        assertThat(cnt.get()).isEqualTo(10);

        cnt.set(0);
        log.truncate(8);

        append(log, 11, buf, 6,6,6, 7,7,7, 8,8,8,8);
        log.forEach((entry,index) -> cnt.incrementAndGet());
        assertThat(cnt.get()).isEqualTo(13);

        cnt.set(0);
        log.forEach((entry,index) -> cnt.incrementAndGet(), 0, 25);
        assertThat(cnt.get()).isEqualTo(13);
    }

    public void testSize(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        assertThat(log.size()).isEqualTo(0);
        LogEntries le=new LogEntries();
        le.add(new LogEntry(5, buf));
        log.append(1, le);

        assertThat(log.size()).isEqualTo(1);
        le.clear().add(new LogEntry(5, buf));
        log.append(2, le);
        assertThat(log.size()).isEqualTo(2);

        le.clear();
        for(int i=3; i <= 8; i++)
            le.add(new LogEntry(5, buf));
        log.append(3, le);
        assertThat(log.size()).isEqualTo(8);
        log.commitIndex(3);
        log.truncate(3); // excluding 3
        assertThat(log.size()).isEqualTo(6);
    }

    public void testSizeInBytes(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        assertThat(log.sizeInBytes()).isEqualTo(0);
        byte[] buf=new byte[50];
        final int NUM=100;
        LogEntries le=new LogEntries();
        for(int i=0; i < NUM; i++)
            le.add(new LogEntry(1, buf));
        long last=log.append(1, le);
        assertThat(last).isEqualTo(NUM);
        long size_in_bytes=log.sizeInBytes();
        assertThat(size_in_bytes).isGreaterThanOrEqualTo(buf.length * NUM);
    }

    public void testInternalCommand(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        assertThat(log.sizeInBytes()).isEqualTo(0);
        byte[] buf=new byte[10];
        LogEntries entries = new LogEntries();

        for (int i = 1; i<=10; i++) {
            LogEntry entry = new LogEntry(i, buf);
            entries.add(entry.internal((i & 1) == 0));
        }

        long last = log.append(1, entries);
        assertThat(last).isEqualTo(entries.size());

        SoftAssertions.assertSoftly(softly -> {
            log.forEach((e, l) -> {
                softly.assertThat(e.internal())
                        .withFailMessage("Entry not internal anymore")
                        .isEqualTo(((l & 1) == 0));
            });
        });
    }


    protected static void append(final Log log, int start_index, final byte[] buf, int... terms) {
        LogEntries le=new LogEntries();
        for(int term: terms)
            le.add(new LogEntry(term, buf));
        log.append(start_index, le);
    }

    protected void assertIndices(int first_applied, int last_applied, int commit_index, int current_term) {
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(log.firstAppended()).as("Incorrect first appended").isEqualTo(first_applied);
            softly.assertThat(log.lastAppended()).as("Incorrect last appended").isEqualTo(last_applied);
            softly.assertThat(log.commitIndex()).as("Incorrect commit index").isEqualTo(commit_index);
            softly.assertThat(log.currentTerm()).as("Incorrect current term").isEqualTo(current_term);
        });
    }

}
