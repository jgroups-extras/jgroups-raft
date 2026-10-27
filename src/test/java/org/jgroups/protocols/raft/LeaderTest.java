package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_CONFLICTING_PREV_TERM;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.stack.Protocol;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL)
public class LeaderTest {

    public void testConflictingPrevTermDoesNotViolateCommitTableInvariant() {
        RAFT raft = new RAFT();
        Address leaderAddr = RAFT.createRaftAddress("A");
        Address followerAddr = RAFT.createRaftAddress("B");
        raft.setAddress(leaderAddr);
        raft.request_table = new RequestTable<>();
        raft.commit_table = new CommitTable(Collections.singletonList(followerAddr), 6);
        raft.createReadOnlyRequestRepository();
        Leader leader = new Leader(raft);

        // Follower previously committed up to 5
        raft.commit_table.update(followerAddr, 5, 6, 5, false);

        // Follower rejects append with FAIL_CONFLICTING_PREV_TERM where result.index() == 5, commit_index == 5
        AppendResult result = new AppendResult(FAIL_CONFLICTING_PREV_TERM, 5, 1).commitIndex(5);

        // When leader handles this response, match_index must not be set below commit_index (5)
        leader.handleAppendEntriesResponse(followerAddr, 1, 0, result);

        CommitTable.Entry entry = raft.commit_table.get(followerAddr);
        assertThat(entry.commitIndex()).isEqualTo(5);
        assertThat(entry.matchIndex())
                .as("matchIndex must be >= commitIndex to maintain CommitTable invariant")
                .isGreaterThanOrEqualTo(entry.commitIndex());
    }

    public void testResendRangeWithTruncatedPredecessorTriggersSnapshot() throws Exception {
        AtomicBoolean snapshotSent = new AtomicBoolean(false);
        AtomicReference<Message> sentMessage = new AtomicReference<>();
        RAFT raft = new RAFT() {
            @Override
            protected void sendSnapshotTo(Address dest) {
                snapshotSent.set(true);
            }
        };
        Address leaderAddr = RAFT.createRaftAddress("A");
        Address followerAddr = RAFT.createRaftAddress("B");
        raft.setAddress(leaderAddr);

        Protocol downProt = new Protocol() {
            @Override
            public Object down(Message msg) {
                sentMessage.set(msg);
                return null;
            }
        };
        raft.setDownProtocol(downProt);

        InMemoryLog log = new InMemoryLog();
        log.init("test-log-" + System.nanoTime(), null);
        byte[] data = new byte[]{1, 2, 3};
        log.append(1, new LogEntries()
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data)));
        log.commitIndex(5);
        log.truncate(5);

        raft.log(log);

        assertThat(raft.log().firstAppended()).isEqualTo(5);
        assertThat(raft.log().get(4)).isNull();
        assertThat(raft.log().get(5)).isNotNull();

        // Call resend range [5..5]
        raft.resend(followerAddr, 5, 5);

        assertThat(snapshotSent.get()).as("Snapshot should have been triggered").isTrue();
        assertThat(sentMessage.get()).as("Should not send AppendEntries with prev_term=0").isNull();
    }

    public void testResendSingleWithTruncatedPredecessorTriggersSnapshot() throws Exception {
        AtomicBoolean snapshotSent = new AtomicBoolean(false);
        AtomicReference<Message> sentMessage = new AtomicReference<>();
        RAFT raft = new RAFT() {
            @Override
            protected void sendSnapshotTo(Address dest) {
                snapshotSent.set(true);
            }
        };
        Address leaderAddr = RAFT.createRaftAddress("A");
        Address followerAddr = RAFT.createRaftAddress("B");
        raft.setAddress(leaderAddr);

        Protocol downProt = new Protocol() {
            @Override
            public Object down(Message msg) {
                sentMessage.set(msg);
                return null;
            }
        };
        raft.setDownProtocol(downProt);

        InMemoryLog log = new InMemoryLog();
        log.init("test-log-single-" + System.nanoTime(), null);
        byte[] data = new byte[]{1, 2, 3};
        log.append(1, new LogEntries()
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data))
                .add(new LogEntry(2, data)));
        log.commitIndex(5);
        log.truncate(5);

        raft.log(log);

        assertThat(raft.log().firstAppended()).isEqualTo(5);
        assertThat(raft.log().get(4)).isNull();
        assertThat(raft.log().get(5)).isNotNull();

        // Call resend single index 5
        raft.resend(followerAddr, 5);

        assertThat(snapshotSent.get()).as("Snapshot should have been triggered").isTrue();
        assertThat(sentMessage.get()).as("Should not send AppendEntries with prev_term=0").isNull();
    }
}
