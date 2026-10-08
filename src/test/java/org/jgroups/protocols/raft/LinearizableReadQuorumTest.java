package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.stack.ProtocolStack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.Test;

/**
 * Verifies that a linearizable read never completes when the leader is in a minority partition.
 * <p>
 * Regression test for the bug where {@code ReadOnlyRequestRepository.Entry} tracked acceptors
 * with a scalar counter, allowing repeated heartbeat responses from a single minority follower
 * to satisfy the majority threshold.
 */
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
                ch.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, GMS.class);
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
}
