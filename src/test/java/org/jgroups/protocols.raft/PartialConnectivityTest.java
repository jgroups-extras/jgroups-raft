package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.raft.testfwk.PartitionedRaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;
import static org.jgroups.tests.harness.BaseRaftElectionTest.waitUntilAllHaveLeaderElected;

@Test(groups= Global.FUNCTIONAL, singleThreaded=true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class PartialConnectivityTest extends BaseRaftElectionTest.ClusterBased<PartitionedRaftCluster> {

    {
        // Create nodes A, B, C, D, E.
        clusterSize = 5;

        // Since it uses a data provider, it needs to execute per method to inject the values.
        recreatePerMethod = true;
    }

    public void testQuorumLossAndRecovery(Class<?> ignore) {
        int id=1;
        View initial=createView(id++, 0, 1, 2, 3, 4);
        cluster.handleView(initial);

        waitUntilLeaderElected(5_000, 0, 1, 2, 3, 4);
        waitUntilAllHaveLeaderElected(rafts(), 10_000);
        List<Address> leaders = leaders();
        assertThat(leaders).hasSize(1);
        Address leader = leaders.get(0);

        System.out.println("leader is " + leader);

        // Nodes D and E do not update their view.
        cluster.handleView(createView(id++, 0, 2));
        cluster.handleView(createView(id++, 1, 2));

        BooleanSupplier bs = () -> IntStream.of(0, 1, 2)
                .mapToObj(this::raft)
                .filter(Objects::nonNull)
                .allMatch(r -> r.leader() == null);
        assertThat(eventually(bs, 5, TimeUnit.SECONDS))
                .as(this::dumpLeaderAndTerms)
                .isTrue();
        assertThat(raft(3).leader()).as("leader should be " + leader + ", but found " + raft(3).leader()).isEqualTo(leader);
        assertThat(raft(4).leader()).as("leader should be " + leader + ", but found " + raft(4).leader()).isEqualTo(leader);

        for (RaftNode n : nodes()) {
            assertThat(n.election().isVotingThreadRunning())
                    .as("election thread should not be running in " + n)
                    .isFalse();
        }

        // Node `E` is the new view coordinator.
        View after = createView(id++, 4, 3, 0, 1, 2);
        System.out.println("after restored network: " + after);
        cluster.handleView(after);

        boolean elected = Util.waitUntilTrue(3000, 200, () -> Stream.of(nodes()).allMatch(n -> n.raft().leader() != null));
        if (election(0).getClass().equals(ELECTION2.class)) {
            assertThat(elected).as("leader was never elected again").isTrue();
            leaders = leaders();
            assertThat(leaders).hasSize(1);
            System.out.println("Leader after restored network: " + leaders.get(0));
        } else {
            assertThat(elected).as("Leader was elected again").isFalse();
            assertThat(election(0)).isInstanceOf(ELECTION.class);
        }
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
