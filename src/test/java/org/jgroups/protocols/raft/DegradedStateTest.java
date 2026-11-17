package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.tests.harness.BaseStateMachineTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

/**
 * Verifies that state machine failures trigger degraded state and that degraded nodes reject new requests.
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class DegradedStateTest extends BaseStateMachineTest<DegradedStateTest.FailingStateMachine> {

    {
        clusterSize = 2;
        recreatePerMethod = true;
    }

    @Override
    protected FailingStateMachine createStateMachine(JChannel ch) {
        return new FailingStateMachine();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.resendInterval(600_000);
    }

    @Override
    protected void afterClusterCreation() throws Exception {
        super.afterClusterCreation();
        assertThat(eventually(() -> leader() != null, 10, TimeUnit.SECONDS))
                .as("Leader election should complete")
                .isTrue();
    }

    /**
     * When {@code state_machine.apply()} throws, the node enters degraded state.
     */
    public void testStateMachineFailureEntersDegradedState() throws Exception {
        RAFT leader = leader();
        assertThat(leader.isLeader()).isTrue();
        assertThat(leader.canHandleRequests()).isTrue();

        stateMachine(0).armFailure(new RuntimeException("disk full"));

        byte[] data = new byte[]{1, 2, 3, 4};
        CompletableFuture<byte[]> f = handle(0).setAsync(data, 0, data.length);

        assertThatThrownBy(() -> f.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("disk full");

        assertThat(leader.canHandleRequests()).isFalse();
    }

    public void testDegradedNodeRejectsNewWrites() throws Exception {
        RAFT leader = leader();
        stateMachine(0).armFailure(new RuntimeException("disk full"));

        byte[] data = new byte[]{1, 2, 3, 4};
        CompletableFuture<byte[]> f = handle(0).setAsync(data, 0, data.length);
        assertThatThrownBy(() -> f.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);

        assertThat(leader.canHandleRequests()).isFalse();

        assertThatThrownBy(() -> handle(0).setAsync(data, 0, data.length))
                .isInstanceOf(DegradedStateException.class);
    }

    public void testNonLeaderRejectsWithRaftLeaderException() {
        RAFT follower = follower();
        assertThat(follower.isLeader()).isFalse();

        byte[] data = new byte[]{1, 2, 3, 4};
        assertThatThrownBy(() -> follower.setAsync(data, 0, data.length))
                .isInstanceOf(RaftLeaderException.class);
    }

    /**
     * A failure in the middle of a batch of commits must stop the batch.
     *
     * <p>
     * One request appends and commits three entries, so the follower applies them in a single pass. The second entry
     * fails: the third must not be applied, and the commit index must stay before the failed entry, so a restart
     * replays it instead of skipping it.
     * </p>
     */
    public void testFailureInBatchStopsApplyingRemainingEntries() {
        int followerIndex = followerIndex();
        RAFT follower = raft(followerIndex);
        FailingStateMachine sm = stateMachine(followerIndex);

        long base = follower.commitIndex();
        assertThat(follower.lastAppended())
                .as("Follower must have nothing pending before the batch")
                .isEqualTo(base);
        int appliedBefore = sm.applied();

        sm.armFailure(2, new RuntimeException("disk full"));

        // Called directly on the follower, so all three entries arrive and commit in the same request.
        // The leader stays idle (long resend interval), nothing else touches the follower's log meanwhile.
        long term = follower.currentTerm();
        byte[] payload = new byte[]{1};
        LogEntries entries = new LogEntries()
                .add(new LogEntry(term, payload), new LogEntry(term, payload), new LogEntry(term, payload));
        follower.impl().handleAppendEntriesRequest(entries, leader().getAddress(), term, base, term, term, base + 3);

        assertThat(follower.canHandleRequests()).isFalse();
        assertThat(sm.applied())
                .as("Only the entry before the failure is applied")
                .isEqualTo(appliedBefore + 1);
        assertThat(follower.commitIndex())
                .as("Commit index stops before the failed entry")
                .isEqualTo(base + 1);
    }

    /**
     * A degraded node must not snapshot its state machine when the log grows past the limit.
     *
     * <p>
     * The failed command may have changed the state before throwing. A snapshot would save that state as the result of
     * the last applied entry and truncate the log, so a restart would replay the failed command on top of its own
     * partial effects.
     * </p>
     */
    public void testDegradedNodeDoesNotSnapshotWhenLogGrows() throws Exception {
        int followerIndex = followerIndex();
        RAFT follower = raft(followerIndex);
        assertThat(hasSnapshot(follower)).isFalse();

        long term = follower.currentTerm();
        long base = follower.commitIndex();
        degradeFollower(followerIndex);
        long firstAppended = follower.log().firstAppended();

        // Any append now exceeds the limit and would trigger a snapshot.
        follower.maxLogSize(1);
        follower.impl().handleAppendEntriesRequest(new LogEntries().add(new LogEntry(term, new byte[]{1})),
                leader().getAddress(), term, base + 1, term, term, base + 1);

        assertThat(follower.lastAppended())
                .as("The degraded follower still appends entries")
                .isEqualTo(base + 2);
        assertThat(hasSnapshot(follower))
                .as("No snapshot is taken while degraded")
                .isFalse();
        assertThat(follower.log().firstAppended())
                .as("The log is not truncated while degraded")
                .isEqualTo(firstAppended);
    }

    /**
     * An explicit snapshot request on a degraded node fails instead of saving state the failed command may have changed.
     */
    public void testDegradedNodeRejectsExplicitSnapshot() throws Exception {
        int followerIndex = followerIndex();
        RAFT follower = raft(followerIndex);
        degradeFollower(followerIndex);

        assertThatThrownBy(follower::snapshot)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(DegradedStateException.class);
        assertThat(hasSnapshot(follower)).isFalse();
    }

    /**
     * Degrades the follower by failing the only entry of a batch committed directly on it.
     */
    private void degradeFollower(int followerIndex) {
        RAFT follower = raft(followerIndex);
        long term = follower.currentTerm();
        long base = follower.commitIndex();

        stateMachine(followerIndex).armFailure(new RuntimeException("disk full"));
        follower.impl().handleAppendEntriesRequest(new LogEntries().add(new LogEntry(term, new byte[]{1})),
                leader().getAddress(), term, base, term, term, base + 1);

        assertThat(follower.canHandleRequests()).isFalse();
    }

    private static boolean hasSnapshot(RAFT raft) throws Exception {
        try (InputStream snapshot = raft.log().getSnapshot()) {
            return snapshot != null;
        }
    }

    private RAFT follower() {
        return raft(followerIndex());
    }

    private int followerIndex() {
        for (int i = 0; i < clusterSize; i++) {
            RAFT r = raft(i);
            if (r != null && !r.isLeader())
                return i;
        }
        throw new IllegalStateException("There are no followers!");
    }

    public static class FailingStateMachine implements StateMachine {
        // Counts every apply call, failed or not, to find the call that must fail.
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger applied = new AtomicInteger();
        private volatile RuntimeException failWith;
        private volatile int failAtCall;

        void armFailure(RuntimeException ex) {
            armFailure(1, ex);
        }

        /**
         * Fails the {@code nth} apply from now, counting from 1. Applies before it succeed.
         */
        void armFailure(int nth, RuntimeException ex) {
            this.failAtCall = calls.get() + nth;
            this.failWith = ex;
        }

        int applied() {
            return applied.get();
        }

        @Override
        public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
            int call = calls.incrementAndGet();
            RuntimeException ex = failWith;
            if (ex != null && call == failAtCall) {
                failWith = null;
                throw ex;
            }
            applied.incrementAndGet();
            return serialize_response ? new byte[0] : null;
        }

        @Override
        public void readContentFrom(DataInput in) {}

        @Override
        public void writeContentTo(DataOutput out) {}
    }
}
