package org.jgroups.protocols.raft.election;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.View;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.BlockingMessageInterceptor;
import org.jgroups.raft.testfwk.PartitionedRaftCluster;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class NetworkPartitionElectionTest extends BaseRaftElectionTest.ClusterBased<PartitionedRaftCluster> {

    {
        clusterSize = 5;

        // Since it uses a data provider, it needs to execute per method to inject the values.
        recreatePerMethod = true;
    }

    public void testNetworkPartitionDuringElection(Class<?> ignore) throws Exception {
        withClusterSize(5);
        createCluster();
        long id = 0;

        View view = createView(id++, 0, 1, 2, 3, 4);

        // We intercept the first `LeaderElected` message.
        AtomicBoolean onlyOnce = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m -> {
            for (Map.Entry<Short, Header> h : m.getHeaders().entrySet()) {
                if (h.getValue() instanceof LeaderElected && onlyOnce.getAndSet(false)) {
                    // Assert that node A was elected
                    LeaderElected le = (LeaderElected) h.getValue();
                    assertThat(le.leader()).isEqualTo(address(0));
                    return true;
                }
            }
            return false;
        });

        cluster.handleView(view);

        LOGGER.info("-- wait command intercept");
        assertThat(RaftTestUtils.eventually(() -> interceptor.numberOfBlockedMessages() > 0, 10, TimeUnit.SECONDS)).isTrue();

        // While the message is in-flight, the cluster splits.
        // The previous coordinator does not have the majority to proceed.
        cluster.handleView(createView(id++, 0, 1));
        cluster.handleView(createView(id++, 2, 3, 4));

        // We can release the elected message.
        interceptor.releaseNext();
        interceptor.assertNoBlockedMessages();

        // Check in all instances that a new leader is elected.
        LOGGER.info("-- waiting for leader in majority partition");
        BaseRaftElectionTest.waitUntilLeaderElected(rafts(), 10_000);

        // Assert that A and B does not have a leader.
        assertThat(raft(0).leader()).isNull();
        assertThat(raft(1).leader()).isNull();

        LOGGER.info("-- elected during the split%n{}", dumpLeaderAndTerms());
        // Store who's the leader before merging.
        assertThat(leaders()).hasSize(1);
        RAFT leader = raft(leaders().get(0));

        LOGGER.info("-- merge partition, leader={}", leader);
        // Join the partitions.
        // Note that the coordinator is different.
        cluster.handleView(createView(id++, 0, 1, 2, 3, 4));

        // Wait until A and B receive the leader information.
        BaseRaftElectionTest.waitUntilAllHaveLeaderElected(rafts(), 10_000);
        LOGGER.info("-- state after merge%n{}", dumpLeaderAndTerms());
    }

    private RAFT raft(Address address) {
        for (RAFT raft : rafts()) {
            if (Objects.equals(address, raft.getAddress()))
                return raft;
        }

        throw new IllegalArgumentException(String.format("Node with address '%s' not present", address));
    }

    @Override
    protected PartitionedRaftCluster createNewMockCluster() {
        return new PartitionedRaftCluster();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.synchronous(true);
    }
}
