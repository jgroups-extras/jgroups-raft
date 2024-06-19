package org.jgroups.tests.election;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.View;
import org.jgroups.protocols.raft.election.LeaderElected;
import org.jgroups.raft.testfwk.BlockingMessageInterceptor;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.tests.harness.RaftAssertion;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class DelayedElectedLeaderMessageTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    /**
     * Use blocking interceptor to capture LeaderElected.
     * Install complete view to succeed in the election process.
     * While the message is blocked, remove the majority of nodes.
     * After new view installed, remove the interceptor.
     * The node should not install the leader.
     */

    private static final byte[] BUF = {};

    {
        createManually = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }

    public void testQuorumLostAfterMessageSent(Class<?> ignore) throws Exception {
        long viewId = 0;
        withClusterSize(5);
        createCluster();

        // Create complete view to successfully elect a node.
        // Use a cluster of 5 nodes.
        View v1 = createView(viewId++, 0, 1, 2, 3, 4);

        // Run asynchronously to allow the voting thread to stop.
        cluster.async(true);

        // Intercept the first `LeaderElected` message.
        AtomicBoolean onlyOnce = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m -> {
            for (Map.Entry<Short, Header> h : m.getHeaders().entrySet()) {
                if (h.getValue() instanceof LeaderElected && onlyOnce.getAndSet(false)) {
                    // Assert the coordinator was elected.
                    LeaderElected le = (LeaderElected) h.getValue();
                    assertThat(le.leader()).isEqualTo(address(0));
                    return true;
                }
            }
            return false;
        });

        // Install view and elect the coordinator.
        cluster.handleView(v1);

        // Intercept the leader elected message.
        System.out.println("-- wait command intercept");
        assertThat(eventually(() -> interceptor.numberOfBlockedMessages() > 0, 10, TimeUnit.SECONDS)).isTrue();

        // Install the new view while the LeaderElected is in-flight.
        // The new view does not have a majority.
        System.out.println("-- install new view without majority");
        View v2 = createView(viewId++, 0, 1);
        cluster.handleView(v2);

        // Release the leader elected message.
        // The node should not install the new leader.
        System.out.println("-- release leader elected message");
        interceptor.releaseNext();
        interceptor.assertNoBlockedMessages();

        // Make sure the leader stays null for the whole time.
        BooleanSupplier bs = () -> Arrays.stream(rafts())
                .anyMatch(r -> r.leader() != null);
        assertThat(eventually(bs, 3, TimeUnit.SECONDS))
                .withFailMessage(this::dumpLeaderAndTerms)
                .isFalse();

        assertThat(bs.getAsBoolean()).isFalse();
        RaftAssertion.assertLeaderlessOperationThrows(() -> raft(0).set(BUF, 0, 0));
    }

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }
}
