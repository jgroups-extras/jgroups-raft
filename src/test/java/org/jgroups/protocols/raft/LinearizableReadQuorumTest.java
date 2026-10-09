package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.stack.ProtocolStack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LinearizableReadQuorumTest {
    private static final Log LOG = LogFactory.getLog(LinearizableReadQuorumTest.class);

    /**
     * In a 3-node cluster (majority=2), isolate the leader from both followers by inserting
     * DISCARD on the two followers. A linearizable read submitted on the isolated leader must
     * never complete while the partition is active.
     */
    public void testLinearizableReadDoesNotCompleteInMinorityPartition() throws Exception {
        JRaftTestCluster<SimpleKVStateMachine> cluster =
                JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, 3);
        try {
            cluster.waitUntilLeaderElected();

            int leaderIdx = cluster.leaderIndex();

            // Isolate the leader: insert DISCARD on both followers so they drop all traffic.
            // The leader stays in a minority partition with no reachable quorum members.
            for (int i = 0; i < 3; i++) {
                if (leaderIdx == i) continue;
                JChannel ch = cluster.channel(i);
                DISCARD discard = new DISCARD().discardAll(true).setAddress(ch.getAddress());
                ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
                LOG.info("Adding discard to channel %s", ch);
            }

            // Submit a linearizable read on the isolated leader asynchronously so the test
            // thread is not blocked indefinitely.
            JGroupsRaftReadCommandOptions linearizable = JGroupsRaftCommandOptions.readOptions()
                    .linearizable(true)
                    .build();
            CompletableFuture<String> readFuture = CompletableFuture.supplyAsync(() ->
                    cluster.leader().read(kv -> kv.handleGet("key"), linearizable));

            // The read must NOT complete: the leader is isolated and cannot reach quorum.
            assertThat(readFuture.isDone()).isFalse();
            assertThatThrownBy(() -> readFuture.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(TimeoutException.class);

            // Heal partition: remove DISCARD from the followers.
            for (int i = 0; i < 3; i++) {
                if (leaderIdx == i) continue;
                cluster.channel(i).getProtocolStack().removeProtocol(DISCARD.class);
            }

            // Without another message, the previous one is never resent.
            assertThat(cluster.leader().<String>read(kv -> kv.handleGet("key")))
                    .isNull();

            // After healing, the pending read must eventually complete successfully.
            assertThat(eventually(readFuture::isDone, 10, TimeUnit.SECONDS)).isTrue();
            assertThat(readFuture.isCompletedExceptionally()).isFalse();
        } finally {
            cluster.close();
        }
    }

    /**
     * Verifies that when a leader is in a minority partition with uncommitted writes,
     * an incoming read operation (which has entries.size() == 0) does NOT commit
     * the pending uncommitted writes without quorum.
     */
    public void testUncommittedWriteNotCommittedByRead() throws Exception {
        JRaftTestCluster<SimpleKVStateMachine> cluster =
                JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, 3);
        try {
            cluster.waitUntilLeaderElected();

            int leaderIdx = cluster.leaderIndex();

            // Isolate the leader: insert DISCARD on both followers so writes cannot reach quorum.
            for (int i = 0; i < 3; i++) {
                if (leaderIdx == i) continue;
                JChannel ch = cluster.channel(i);
                DISCARD discard = new DISCARD().discardAll(true).setAddress(ch.getAddress());
                ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
            }

            // Submit an uncommitted write on the isolated leader asynchronously.
            CompletableFuture<Void> writeFuture = CompletableFuture.supplyAsync(() ->
                    cluster.leader().write(kv -> kv.handlePut("k1", "v1")));

            // Wait until the leader has appended k1 to its log (lastAppended == 1)
            RAFT leaderRaft = cluster.channel(leaderIdx).getProtocolStack().findProtocol(RAFT.class);
            assertThat(eventually(() -> leaderRaft.lastAppended() == 1, 5, TimeUnit.SECONDS)).isTrue();

            // The write cannot reach a majority, so it must NOT be completed.
            assertThat(writeFuture.isDone()).isFalse();

            // Now submit a read operation on the isolated leader asynchronously.
            CompletableFuture<String> readFuture = CompletableFuture.supplyAsync(() ->
                    cluster.leader().read(kv -> kv.handleGet("k1")));

            // The linearizable read must NOT complete while isolated.
            // Waiting for the read to time out ensures the leader's event loop has processed the request.
            assertThatThrownBy(() -> readFuture.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(TimeoutException.class);

            // CRITICAL: The pending write must STILL be uncommitted.
            assertThat(leaderRaft.commitIndex())
                    .as("Leader must not advance commitIndex for uncommitted entries")
                    .isEqualTo(0L);
            assertThat(writeFuture.isDone())
                    .as("Uncommitted write must NOT be completed by an incoming read while isolated")
                    .isFalse();

            // Heal partition: remove DISCARD from the followers.
            for (int i = 0; i < 3; i++) {
                if (leaderIdx == i) continue;
                cluster.channel(i).getProtocolStack().removeProtocol(DISCARD.class);
            }

            // After healing, replicate a write to advance and sync the cluster.
            cluster.leader().write(kv -> kv.handlePut("k2", "v2"));

            // Now both the write and read should complete successfully.
            assertThat(eventually(writeFuture::isDone, 10, TimeUnit.SECONDS)).isTrue();
            assertThat(eventually(readFuture::isDone, 10, TimeUnit.SECONDS)).isTrue();
            assertThat(writeFuture.isCompletedExceptionally()).isFalse();
            assertThat(readFuture.isCompletedExceptionally()).isFalse();
        } finally {
            cluster.close();
        }
    }
}
