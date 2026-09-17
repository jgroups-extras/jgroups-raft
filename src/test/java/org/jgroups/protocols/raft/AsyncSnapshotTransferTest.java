package org.jgroups.protocols.raft;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.raft.AsyncSnapshot;
import org.jgroups.raft.SnapshotHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.tests.harness.BaseStateMachineTest;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Integration tests verifying end-to-end chunked snapshot transfer when the state machine implements {@link AsyncSnapshot}.
 *
 * <p>
 * The async snapshot path uses chunked transfer (metadata + chunk request/response messages) instead of the single-message
 * {@code InstallSnapshotRequest} used by the synchronous path. These tests verify that a lagging follower receives the
 * snapshot via chunks, installs it, and resumes normal replication.
 * </p>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class AsyncSnapshotTransferTest extends BaseStateMachineTest<AsyncSnapshotTransferTest.AsyncCounterStateMachine> {

    private int term = 10;

    {
        createManually = true;
    }

    @Override
    protected AsyncCounterStateMachine createStateMachine(JChannel ch) {
        return new AsyncCounterStateMachine();
    }

    @Override
    protected Protocol[] baseProtocolStackForNode(String name) {
        return Util.getTestStack(createNewRaft(name), createRedirect());
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.snapshot_chunk_size = 32;
        raft.snapshot_batch_size = 4;
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        destroyCluster();
    }

    /**
     * Follower C is removed, leader commits entries and snapshots (truncating the log).
     * When C restarts, its log is empty so the leader must send a snapshot via chunked transfer.
     * After installation, C's state machine matches the leader's, and subsequent entries replicate normally.
     */
    public void testFollowerCatchesUpViaChunkedSnapshot() throws Exception {
        init();
        close(2);

        RAFT leader = raft(0);
        for (int i = 1; i <= 5; i++) {
            byte[] data = new byte[Integer.BYTES];
            Bits.writeInt(i, data, 0);
            leader.set(data, 0, data.length, 5, SECONDS);
        }
        assertStateMachineEventuallyMatch(0, 1);

        leader.snapshot();
        waitForSnapshotCompletion(leader);

        createCluster();

        assertStateMachineEventuallyMatch(0, 1, 2);

        assertThat(stateMachine(2).counter())
                .isEqualTo(stateMachine(0).counter());

        assertThat(raft(2).numSnapshotReceived())
                .as("Follower C should have received a snapshot")
                .isGreaterThanOrEqualTo(1);
    }

    /**
     * After a chunked snapshot installation, the follower should resume normal replication.
     * New entries committed after the snapshot should reach the follower without another snapshot transfer.
     */
    public void testReplicationResumesAfterChunkedSnapshot() throws Exception {
        init();
        close(2);

        RAFT leader = raft(0);
        for (int i = 1; i <= 5; i++) {
            byte[] data = new byte[Integer.BYTES];
            Bits.writeInt(i, data, 0);
            leader.set(data, 0, data.length, 5, SECONDS);
        }
        assertStateMachineEventuallyMatch(0, 1);

        leader.snapshot();
        waitForSnapshotCompletion(leader);

        createCluster();

        assertStateMachineEventuallyMatch(0, 1, 2);
        long snapshotsAfterCatchUp = raft(2).numSnapshotReceived();

        for (int i = 6; i <= 10; i++) {
            byte[] data = new byte[Integer.BYTES];
            Bits.writeInt(i, data, 0);
            leader.set(data, 0, data.length, 5, SECONDS);
        }

        assertStateMachineEventuallyMatch(0, 1, 2);

        assertThat(stateMachine(2).counter())
                .isEqualTo(stateMachine(0).counter());

        assertThat(raft(2).numSnapshotReceived())
                .as("No additional snapshot should be needed for entries after catch-up")
                .isEqualTo(snapshotsAfterCatchUp);
    }

    /**
     * Automatic snapshot triggered by exceeding max_log_size should work with the async path.
     * The follower that missed entries catches up via the auto-triggered snapshot.
     */
    public void testAutomaticSnapshotTriggersChunkedTransfer() throws Exception {
        init();
        close(2);

        RAFT leader = raft(0);
        leader.maxLogSize(20);

        for (int i = 1; i <= 20; i++) {
            byte[] data = new byte[Integer.BYTES];
            Bits.writeInt(i, data, 0);
            leader.set(data, 0, data.length, 5, SECONDS);
        }

        waitForSnapshotCompletion(leader);
        assertStateMachineEventuallyMatch(0, 1);

        createCluster();

        assertStateMachineEventuallyMatch(0, 1, 2);

        assertThat(stateMachine(2).counter())
                .isEqualTo(stateMachine(0).counter());
    }

    public void testSnapshotBlocksUntilPersisted() throws Exception {
        init();

        RAFT leader = raft(0);
        for (int i = 1; i <= 5; i++) {
            byte[] data = new byte[Integer.BYTES];
            Bits.writeInt(i, data, 0);
            leader.set(data, 0, data.length, 5, SECONDS);
        }

        // Assert the method only returns after the full snapshot taking procedure is complete.
        leader.snapshot();

        assertThat(leader.log().firstAppended())
                .as("Log should be truncated after snapshot() returns")
                .isGreaterThan(0);
        assertThat(leader.log().snapshotSize())
                .as("Snapshot should be persisted after snapshot() returns")
                .isGreaterThan(0);
    }

    /**
     * When the leader's snapshot data is missing (e.g., lost on restart) but the log is already truncated,
     * the leader must force a new snapshot creation before it can serve a lagging follower.
     */
    public void testForcedSnapshotOnMissingSnapshotData() throws Exception {
        init();
        close(2);

        RAFT leader = raft(0);
        for (int i = 1; i <= 5; i++) {
            byte[] data = new byte[Integer.BYTES];
            Bits.writeInt(i, data, 0);
            leader.set(data, 0, data.length, 5, SECONDS);
        }
        assertStateMachineEventuallyMatch(0, 1);

        leader.snapshot();
        waitForSnapshotCompletion(leader);

        assertThat(leader.log().firstAppended())
                .as("Log should be truncated after snapshot")
                .isGreaterThan(0);

        // Force losing the snapshot.
        leader.log().setSnapshot((InputStream) null);

        assertThat(leader.log().snapshotSize())
                .as("Snapshot data should be cleared")
                .isEqualTo(0);

        createCluster();

        assertStateMachineEventuallyMatch(0, 1, 2);

        assertThat(stateMachine(2).counter())
                .isEqualTo(stateMachine(0).counter());
    }

    private static void waitForSnapshotCompletion(RAFT raft) {
        assertThat(eventually(() -> raft.numSnapshots() > 0 && raft.log().firstAppended() > 0, 10, SECONDS))
                .as("Snapshot should complete: numSnapshots=%d, firstAppended=%d", raft.numSnapshots(), raft.log().firstAppended())
                .isTrue();
    }

    private void init() throws Exception {
        withClusterSize(3);
        createCluster();

        int t = term++;
        for (JChannel ch : channels()) {
            raft(ch).setLeaderAndTerm(address(0), t);
        }

        assertThat(raft(0).isLeader()).isTrue();
    }

    /**
     * A state machine that maintains a running sum and supports async snapshots.
     *
     * <p>
     * {@link #prepareSnapshot()} captures the current counter value into a handle that can be serialized on a background
     * thread. The state machine continues accepting mutations after the handle is captured.
     * </p>
     */
    static class AsyncCounterStateMachine implements StateMachine, AsyncSnapshot {
        private int counter;

        int counter() {
            return counter;
        }

        @Override
        public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
            int val = Bits.readInt(data, offset);
            counter += val;
            if (!serialize_response)
                return null;
            byte[] retval = new byte[Integer.BYTES];
            Bits.writeInt(counter, retval, 0);
            return retval;
        }

        @Override
        public void writeContentTo(DataOutput out) throws Exception {
            out.writeInt(counter);
        }

        @Override
        public void readContentFrom(DataInput in) {
            try {
                counter = in.readInt();
            } catch (IOException ignored) { }
        }

        @Override
        public SnapshotHandle prepareSnapshot() {
            int captured = counter;
            return new SnapshotHandle() {
                @Override
                public void writeTo(DataOutput out) throws Exception {
                    out.writeInt(captured);
                }

                @Override
                public void release() { }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AsyncCounterStateMachine that)) return false;
            return counter == that.counter;
        }

        @Override
        public int hashCode() {
            return Objects.hash(counter);
        }

        @Override
        public String toString() {
            return "AsyncCounterStateMachine{counter=" + counter + "}";
        }
    }
}
