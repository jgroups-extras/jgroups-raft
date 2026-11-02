package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_CONFLICTING_PREV_TERM;
import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_ENTRY_NOT_FOUND;
import static org.jgroups.protocols.raft.AppendResult.Result.OK;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.CommitTable.Send;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

/**
 * The leader keeps one entry per follower, updated by the follower's responses, and decides what to send it next.
 *
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL)
public class CommitTableTest {
    private static final AtomicInteger LOG_ID = new AtomicInteger();

    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D"), e=Util.createRandomAddress("E");

    public void testAddition() throws Exception {
        CommitTable table=new CommitTable(Arrays.asList(a,b,c), log(4, 4));
        assertThat(table.keys()).hasSize(3);

        List<Address> mbrs=Arrays.asList(b, c, d, e);
        table.adjust(mbrs);
        Set<Address> keys=table.keys();
        assertThat(keys).isEqualTo(new HashSet<>(mbrs));
    }

    public void testNewFollowerStartsAfterLeaderLastEntry() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(5, 5));

        CommitTable.Entry entry = table.get(b);
        assertThat(entry.nextIndex()).isEqualTo(6);
        assertThat(entry.matchIndex()).isZero();
        assertThat(entry.commitIndex()).isZero();
    }

    public void testNewFollowerReceivesLeaderLastEntry() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(5, 5));

        // The leader does not know what the follower has, it starts with its last entry.
        assertThat(table.resend(b)).isEqualTo(new Send.Entries(5, 5));
    }

    public void testAcceptedAdvancesIndexes() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(5, 5));

        assertThat(table.received(b, new AppendResult(OK, 3).commitIndex(2))).isInstanceOf(Send.Nothing.class);

        CommitTable.Entry entry = table.get(b);
        assertThat(entry.matchIndex()).isEqualTo(3);
        assertThat(entry.nextIndex()).isEqualTo(4);
        assertThat(entry.commitIndex()).isEqualTo(2);
    }

    public void testLateAcceptanceDoesNotMoveIndexesBack() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(5, 5));

        table.received(b, new AppendResult(OK, 4).commitIndex(4));
        // The response to an older request arrives after the newer one.
        table.received(b, new AppendResult(OK, 3).commitIndex(3));

        CommitTable.Entry entry = table.get(b);
        assertThat(entry.matchIndex()).isEqualTo(4);
        assertThat(entry.commitIndex()).isEqualTo(4);
    }

    public void testFollowerBehindReceivesMissingEntries() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(5, 5));
        table.received(b, new AppendResult(OK, 3).commitIndex(3));

        assertThat(table.resend(b)).isEqualTo(new Send.Entries(4, 5));
    }

    public void testRejectingFollowerCatchesUpFromItsLastEntry() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));

        // The follower's log ends at 5, it rejects the request for entry 8.
        Send send = table.received(b, rejection(5, 7));
        assertThat(send).isEqualTo(new Send.Entries(6, 8));
    }

    public void testOneCatchUpUntilFollowerAccepts() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));

        // The follower rejects the request for entry 7.
        assertThat(table.received(b, rejection(5, 6)))
              .isEqualTo(new Send.Entries(6, 8));

        // The request for entry 8 was sent before the catch-up, its rejection does not send the catch-up again.
        assertThat(table.received(b, rejection(5, 7)))
              .isInstanceOf(Send.Nothing.class);
    }

    public void testLostCatchUpResentWhenFollowerRejectsLaterRequest() throws Exception {
        Log log = log(8, 8);
        CommitTable table = new CommitTable(List.of(b), log);
        assertThat(table.received(b, rejection(5, 6))).isEqualTo(new Send.Entries(6, 8));

        // The catch-up is lost. The leader appends entry 9, and the follower rejects its request, which was sent after
        // the catch-up. The follower would have accepted it with the catch-up, so the catch-up is sent again.
        log.append(9, new LogEntries().add(new LogEntry(1, new byte[] {9})));
        assertThat(table.received(b, rejection(5, 8)))
              .isEqualTo(new Send.Entries(6, 9));
    }

    public void testSilentFollowerReceivesHeartbeatsUntilItAnswers() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));
        table.received(b, new AppendResult(OK, 5).commitIndex(5));
        assertThat(table.resend(b)).isEqualTo(new Send.Entries(6, 8));

        // The follower does not answer, it may be unreachable. Resending the entries only adds traffic.
        assertThat(table.resend(b)).isEqualTo(new Send.Heartbeat(8));
        assertThat(table.resend(b)).isEqualTo(new Send.Heartbeat(8));

        // The follower answers a heartbeat, its log still ends at 5.
        assertThat(table.received(b, rejection(5, 8))).isEqualTo(new Send.Entries(6, 8));
    }

    public void testFollowerAcceptingCatchUpAllowsTheNextOne() throws Exception {
        Log log = log(8, 8);
        CommitTable table = new CommitTable(List.of(b), log);
        table.received(b, rejection(5, 7));
        table.received(b, new AppendResult(OK, 8).commitIndex(8));

        // The leader appends entries 9 and 10, and the follower misses them.
        log.append(9, new LogEntries().add(new LogEntry(1, new byte[] {9})).add(new LogEntry(1, new byte[] {10})));
        assertThat(table.received(b, rejection(8, 9)))
              .isEqualTo(new Send.Entries(9, 10));
    }

    public void testFollowerBehindSnapshotReceivesSnapshot() throws Exception {
        // The leader's log starts at 8, the entries before it are in the snapshot.
        CommitTable table = new CommitTable(List.of(b), log(10, 10, 8));

        assertThat(table.received(b, rejection(3, 9)))
              .isInstanceOf(Send.Snapshot.class);
    }

    public void testSilentFollowerBehindSnapshotReceivesHeartbeatsUntilItAnswers() throws Exception {
        // The leader's log starts at 8, the entries before it are in the snapshot.
        CommitTable table = new CommitTable(List.of(b), log(10, 10, 8));
        assertThat(table.received(b, rejection(3, 9))).isInstanceOf(Send.Snapshot.class);

        // The follower does not answer, sending the whole snapshot again only adds traffic.
        assertThat(table.resend(b)).isEqualTo(new Send.Heartbeat(10));

        // The follower installs the snapshot, and receives the entries after it.
        table.received(b, new AppendResult(OK, 8).commitIndex(8));
        assertThat(table.resend(b)).isEqualTo(new Send.Entries(9, 10));
    }

    public void testConflictingFollowerReceivesOneEntryAtATime() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));

        // The follower's entry 6 has another term, it deleted it and the entries after it.
        assertThat(table.received(b, new AppendResult(FAIL_CONFLICTING_PREV_TERM, 6, 6).commitIndex(4)))
              .isInstanceOf(Send.Nothing.class);

        assertThat(table.resend(b)).isEqualTo(new Send.Entries(6, 6));
    }

    public void testConflictKeepsMatchIndexAtCommitIndex() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));
        table.received(b, new AppendResult(OK, 5).commitIndex(5));

        // The conflict is at the follower's commit index, entries up to it are never deleted.
        table.received(b, new AppendResult(FAIL_CONFLICTING_PREV_TERM, 5, 5).commitIndex(5));

        CommitTable.Entry entry = table.get(b);
        assertThat(entry.commitIndex()).isEqualTo(5);
        assertThat(entry.matchIndex()).isGreaterThanOrEqualTo(entry.commitIndex());
    }

    public void testFollowerWithEveryEntryReceivesHeartbeatForCommits() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(5, 5));
        table.received(b, new AppendResult(OK, 5).commitIndex(3));

        assertThat(table.resend(b)).isEqualTo(new Send.Heartbeat(5));
    }

    public void testFollowerInSyncReceivesNothing() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(5, 5));
        table.received(b, new AppendResult(OK, 5).commitIndex(5));

        assertThat(table.resend(b)).isInstanceOf(Send.Nothing.class);
    }

    public void testUncommittedEntriesResentToCollectAcknowledgements() throws Exception {
        // The leader has entries 4 and 5 it did not commit yet.
        CommitTable table = new CommitTable(List.of(b), log(5, 3));
        table.received(b, new AppendResult(OK, 5).commitIndex(3));

        // https://github.com/jgroups-extras/jgroups-raft/issues/30
        assertThat(table.resend(b)).isEqualTo(new Send.Entries(4, 5));
    }

    public void testFollowerBehindCommitLearnsItUpToItsLastAcknowledgedEntry() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));
        // The follower acknowledged up to 6, its acknowledgements of 7 and 8 are on their way.
        table.received(b, new AppendResult(OK, 6).commitIndex(4));

        // The follower commits up to the entry the heartbeat checks, it cannot commit entries it did not acknowledge.
        assertThat(table.committed(b)).isEqualTo(new Send.Heartbeat(6));
    }

    public void testFollowerAtLeaderCommitReceivesNothingOnCommit() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));
        table.received(b, new AppendResult(OK, 8).commitIndex(8));

        assertThat(table.committed(b)).isInstanceOf(Send.Nothing.class);
    }

    public void testFollowerCommittedUpToItsAcknowledgedEntryReceivesNothingOnCommit() throws Exception {
        CommitTable table = new CommitTable(List.of(b), log(8, 8));
        // The follower already committed every entry it acknowledged.
        table.received(b, new AppendResult(OK, 6).commitIndex(6));

        assertThat(table.committed(b)).isInstanceOf(Send.Nothing.class);
    }

    public void testFollowerWithAcknowledgedEntryInSnapshotReceivesNothingOnCommit() throws Exception {
        // The leader's log starts at 8, the entries before it are in the snapshot.
        CommitTable table = new CommitTable(List.of(b), log(10, 10, 8));
        table.received(b, new AppendResult(OK, 5).commitIndex(3));

        // The heartbeat cannot check entry 5, the resend round brings the follower up to date.
        assertThat(table.committed(b)).isInstanceOf(Send.Nothing.class);
    }

    /**
     * A follower's rejection of a request whose previous entry it does not have.
     *
     * @param lastAppended The follower's last entry.
     * @param prevIndex    The index of the entry before the request's entries.
     */
    private static AppendResult rejection(long lastAppended, long prevIndex) {
        return new AppendResult(FAIL_ENTRY_NOT_FOUND, lastAppended, prevIndex).commitIndex(lastAppended);
    }

    private static Log log(int lastAppended, int commitIndex) throws Exception {
        return log(lastAppended, commitIndex, 1);
    }

    /**
     * Creates the leader's log with entries in term 1.
     *
     * @param firstAppended The first entry kept in the log, the entries before it are in the snapshot.
     */
    private static Log log(int lastAppended, int commitIndex, int firstAppended) throws Exception {
        Log log = new InMemoryLog();
        log.init("commit-table-test-" + LOG_ID.incrementAndGet(), null);
        LogEntries entries = new LogEntries();
        for (int i = 1; i <= lastAppended; i++)
            entries.add(new LogEntry(1, new byte[] {(byte) i}));
        log.append(1, entries);
        log.commitIndex(commitIndex);
        if (firstAppended > 1)
            log.truncate(firstAppended);
        return log;
    }
}
