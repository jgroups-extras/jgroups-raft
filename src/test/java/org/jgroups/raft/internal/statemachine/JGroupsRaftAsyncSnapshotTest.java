package org.jgroups.raft.internal.statemachine;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.AsyncSnapshot;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.SnapshotHandle;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.api.JRaftTestCluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a v2 state machine with {@link AsyncSnapshot} operates correctly.
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftAsyncSnapshotTest {

    private JRaftTestCluster<CounterStateMachine> cluster;

    @AfterMethod
    protected void tearDown() throws Exception {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    public void testFollowerCatchesUpViaAsyncChunkedSnapshot() throws Exception {
        cluster = JRaftTestCluster.create(CounterStateMachineImpl::new, CounterStateMachine.class, 3);
        cluster.waitUntilLeaderElected();

        // We stop one of the node.
        // It'll receive the snapshot once restarted.
        cluster.stop(2);

        // Submit operations to the remaining nodes.
        JGroupsRaft<CounterStateMachine> leader = cluster.leader();
        for (int i = 1; i <= 5; i++) {
            leader.write(sm -> sm.add(1));
        }

        // Take a snapshot at the leader.
        int leaderIdx = cluster.leaderIndex();
        RAFT leaderRaft = cluster.raftProtocol(leaderIdx);
        leaderRaft.snapshot();

        assertThat(leaderRaft.log().firstAppended())
                .as("Log should be truncated after snapshot")
                .isGreaterThan(0);

        // The remaining node should start and receive the snapshot from the leader.
        cluster.restart(2);

        RAFT followerRaft = cluster.raftProtocol(2);
        assertThat(eventually(() -> cluster.stateMachine(2).get() == cluster.stateMachine(leaderIdx).get(), 10, SECONDS))
                .as("Follower should catch up via snapshot transfer")
                .isTrue();

        assertThat(followerRaft.numSnapshotReceived())
                .as("Follower should have received a snapshot")
                .isGreaterThanOrEqualTo(1);
    }

    @JGroupsRaftStateMachine
    interface CounterStateMachine {

        @StateMachineWrite(id = 1)
        Void add(int value);

        @StateMachineRead(id = 2)
        int get();
    }

    static class CounterStateMachineImpl implements CounterStateMachine, AsyncSnapshot {

        @StateMachineField(order = 0)
        private int counter;

        @Override
        public Void add(int value) {
            counter += value;
            return null;
        }

        @Override
        public int get() {
            return counter;
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
        public void readContentFrom(DataInput in) {
            try {
                counter = in.readInt();
            } catch (IOException ignored) { }
        }
    }
}
