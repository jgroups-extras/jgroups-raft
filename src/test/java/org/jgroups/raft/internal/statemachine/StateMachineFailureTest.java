package org.jgroups.raft.internal.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.exceptions.JGroupsRaftException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a failure inside a state machine method is never turned into a regular response.
 *
 * <p>
 * The state machine must be deterministic. A method that throws means the local state may have diverged from the
 * cluster, so the node must stop serving instead of moving on to the next entry.
 * </p>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class StateMachineFailureTest {

    // Reads the local state machine directly, so it works on any node, degraded or not.
    private static final JGroupsRaftReadCommandOptions LOCAL_READ = JGroupsRaftReadCommandOptions.options()
            .linearizable(false)
            .build();

    private JRaftTestCluster<Counter> cluster;

    @AfterMethod(alwaysRun = true)
    protected void destroy() throws Exception {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    public void testWriteFailureOnLeaderEntersDegradedState() {
        cluster = JRaftTestCluster.create(CounterImpl::new, Counter.class, 3);
        cluster.waitUntilLeaderElected();

        int leaderIndex = cluster.leaderIndex();
        JGroupsRaft<Counter> leader = cluster.raft(leaderIndex);
        leader.write(c -> c.add(1));

        // Only the leader fails, the followers apply the command normally.
        counter(leaderIndex).failNextWrite(new IllegalStateException("disk full"));

        assertThatThrownBy(() -> leader.write(c -> c.add(1)))
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("disk full");

        assertThat(cluster.raftProtocol(leaderIndex).canHandleRequests())
                .as("Leader must stop handling requests after a failed apply")
                .isFalse();
    }

    public void testCheckedExceptionOnLeaderEntersDegradedState() {
        cluster = JRaftTestCluster.create(CounterImpl::new, Counter.class, 3);
        cluster.waitUntilLeaderElected();

        int leaderIndex = cluster.leaderIndex();
        JGroupsRaft<Counter> leader = cluster.raft(leaderIndex);
        leader.write(c -> c.add(1));

        counter(leaderIndex).failNextWrite(new IOException("disk full"));

        assertThatThrownBy(() -> leader.write(c -> c.add(1)))
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(JGroupsRaftException.class)
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("disk full");

        assertThat(cluster.raftProtocol(leaderIndex).canHandleRequests())
                .as("Leader must stop handling requests after a failed apply")
                .isFalse();
    }

    public void testWriteFailureOnFollowerStopsApplying() {
        cluster = JRaftTestCluster.create(CounterImpl::new, Counter.class, 3);
        cluster.waitUntilLeaderElected();

        int leaderIndex = cluster.leaderIndex();
        int failingIndex = degradeFollower(leaderIndex);
        int healthyIndex = (leaderIndex + 2) % 3;

        // More commits reach the degraded follower. It must not retry the failed entry nor apply the next ones.
        long leaderCommit = writeAndWaitCommit(leaderIndex, healthyIndex);

        assertThat(localValue(leaderIndex)).isEqualTo(5);
        assertThat(localValue(healthyIndex)).isEqualTo(5);
        assertThat(localValue(failingIndex))
                .as("Degraded follower must keep the state it had before the failure")
                .isEqualTo(1);
        assertThat(cluster.raft(failingIndex).state().commitIndex()).isLessThan(leaderCommit);
        assertThat(cluster.raftProtocol(failingIndex).canHandleRequests()).isFalse();
    }

    public void testDegradedFollowerIsNotReady() {
        cluster = JRaftTestCluster.create(CounterImpl::new, Counter.class, 3);
        cluster.waitUntilLeaderElected();

        int leaderIndex = cluster.leaderIndex();
        int failingIndex = degradeFollower(leaderIndex);
        int healthyIndex = (leaderIndex + 2) % 3;

        // Later messages from the leader let the degraded follower learn the leader again.
        // Knowing the leader must not be enough to report the node as ready.
        writeAndWaitCommit(leaderIndex, healthyIndex);

        JGroupsRaft<Counter> degraded = cluster.raft(failingIndex);
        assertThat(degraded.healthCheck().isNodeLive())
                .as("Degraded node is still running")
                .isTrue();
        assertThat(degraded.healthCheck().isNodeReady())
                .as("Degraded node must not receive traffic")
                .isFalse();
        assertThat(cluster.raft(healthyIndex).healthCheck().isNodeReady()).isTrue();
    }

    public void testReadFailureFailsOnlyTheRequest() throws Exception {
        cluster = JRaftTestCluster.create(CounterImpl::new, Counter.class, 3);
        cluster.waitUntilLeaderElected();

        int leaderIndex = cluster.leaderIndex();
        JGroupsRaft<Counter> leader = cluster.raft(leaderIndex);
        leader.write(c -> c.add(1));

        // An Error is the worst case: it is not an Exception and must still complete the request.
        counter(leaderIndex).failNextRead(new OutOfMemoryError("simulated"));

        // Read in another thread, a request that is never completed would block the test forever.
        CompletableFuture<Integer> read = CompletableFuture.supplyAsync(() -> leader.read(Counter::get));
        assertThatThrownBy(() -> read.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(OutOfMemoryError.class)
                .hasRootCauseMessage("simulated");

        // A read does not change the state, so the replicas cannot diverge and the node keeps serving.
        assertThat(cluster.raftProtocol(leaderIndex).canHandleRequests()).isTrue();
        leader.write(c -> c.add(1));
        assertThat(leader.read(Counter::get)).isEqualTo(2);
    }

    /**
     * Makes one follower fail while applying a write that the leader and the other follower commit.
     *
     * <p>
     * All nodes apply a first write before the failure, so every state machine holds the value 1. The leader and the
     * other follower are a majority, so the client never sees the failure.
     * </p>
     *
     * @return The index of the degraded follower, the other follower is at {@code (leaderIndex + 2) % 3}.
     */
    private int degradeFollower(int leaderIndex) {
        int failingIndex = (leaderIndex + 1) % 3;
        JGroupsRaft<Counter> leader = cluster.raft(leaderIndex);

        leader.write(c -> c.add(1));
        assertThat(eventually(() -> localValue(failingIndex) == 1, 10, TimeUnit.SECONDS))
                .as("All nodes must apply the first write")
                .isTrue();

        counter(failingIndex).failNextWrite(new IllegalStateException("disk full"));

        // The second write carries the commit of the first to the followers.
        leader.write(c -> c.add(1));
        leader.write(c -> c.add(1));
        assertThat(eventually(() -> !cluster.raftProtocol(failingIndex).canHandleRequests(), 10, TimeUnit.SECONDS))
                .as("Follower must enter degraded state after a failed apply")
                .isTrue();
        return failingIndex;
    }

    /**
     * Submits two more writes and waits until the healthy follower commits everything the leader committed.
     *
     * @return The leader commit index after the writes.
     */
    private long writeAndWaitCommit(int leaderIndex, int healthyIndex) {
        JGroupsRaft<Counter> leader = cluster.raft(leaderIndex);
        leader.write(c -> c.add(1));
        leader.write(c -> c.add(1));

        long leaderCommit = leader.state().commitIndex();
        assertThat(eventually(() -> cluster.raft(healthyIndex).state().commitIndex() == leaderCommit, 10, TimeUnit.SECONDS))
                .as("Healthy follower must keep committing")
                .isTrue();
        return leaderCommit;
    }

    private int localValue(int index) {
        return cluster.raft(index).read(Counter::get, LOCAL_READ);
    }

    private CounterImpl counter(int index) {
        return (CounterImpl) cluster.stateMachine(index);
    }

    @JGroupsRaftStateMachine
    interface Counter {

        @StateMachineWrite(id = 1)
        Void add(int delta);

        @StateMachineRead(id = 2)
        int get();
    }

    static final class CounterImpl implements Counter {

        @StateMachineField(order = 0)
        int value;

        // Armed by the test thread, consumed by the thread applying the command.
        private volatile Throwable failure;

        private volatile Throwable readFailure;

        void failNextWrite(Throwable t) {
            this.failure = t;
        }

        void failNextRead(Throwable t) {
            this.readFailure = t;
        }

        @Override
        public Void add(int delta) {
            Throwable t = failure;
            if (t != null) {
                failure = null;
                sneakyThrow(t);
            }
            value += delta;
            return null;
        }

        @Override
        public int get() {
            Throwable t = readFailure;
            if (t != null) {
                readFailure = null;
                sneakyThrow(t);
            }
            return value;
        }

        // Throws checked exceptions without declaring them, so the test lambdas passed to write() stay simple.
        // The adapter sees the same thing as with a method declaring the exception in its signature.
        @SuppressWarnings("unchecked")
        private static <E extends Throwable> void sneakyThrow(Throwable t) throws E {
            throw (E) t;
        }
    }
}
