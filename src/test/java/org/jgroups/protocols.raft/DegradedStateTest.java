package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.tests.harness.BaseStateMachineTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
        raft(0).setLeaderAndTerm(address(0));
        raft(1).setLeaderAndTerm(address(0));
    }

    /**
     * When {@code state_machine.apply()} throws, the node enters degraded state.
     */
    public void testStateMachineFailureEntersDegradedState() throws Exception {
        RAFT leader = raft(0);
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
        RAFT leader = raft(0);
        stateMachine(0).armFailure(new RuntimeException("disk full"));

        byte[] data = new byte[]{1, 2, 3, 4};
        CompletableFuture<byte[]> f = handle(0).setAsync(data, 0, data.length);
        assertThatThrownBy(() -> f.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);

        assertThat(leader.canHandleRequests()).isFalse();

        assertThatThrownBy(() -> handle(0).setAsync(data, 0, data.length))
                .isInstanceOf(DegradedStateException.class);
    }

    public void testNonLeaderRejectsWithRaftLeaderException() throws Exception {
        RAFT follower = raft(1);
        assertThat(follower.isLeader()).isFalse();

        byte[] data = new byte[]{1, 2, 3, 4};
        assertThatThrownBy(() -> follower.setAsync(data, 0, data.length))
                .isInstanceOf(RaftLeaderException.class);
    }

    public static class FailingStateMachine implements StateMachine {
        private volatile RuntimeException failWith;

        void armFailure(RuntimeException ex) {
            this.failWith = ex;
        }

        @Override
        public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
            RuntimeException ex = failWith;
            if (ex != null) {
                failWith = null;
                throw ex;
            }
            return serialize_response ? new byte[0] : null;
        }

        @Override
        public void readContentFrom(DataInput in) {}

        @Override
        public void writeContentTo(DataOutput out) {}
    }
}
