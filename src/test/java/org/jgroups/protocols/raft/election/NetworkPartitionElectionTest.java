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

import java.util.Arrays;
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

        // We intercept the first `LeaderElected` message announcing the election.
        // The announcement goes to all members; answers to members asking for the leader are unicast and pass through.
        AtomicBoolean onlyOnce = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m -> {
            for (Map.Entry<Short, Header> h : m.getHeaders().entrySet()) {
                if (m.getDest() == null && h.getValue() instanceof LeaderElected && onlyOnce.getAndSet(false)) {
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

    /**
     * A merged cluster elects a leader even when its coordinator missed the old leader stepping down.
     * <p>
     * Mirrors a bridge partition: the merge coordinator still trusts the old leader, while every other member knows
     * there is no leader. Raft expects a new election once followers stop hearing from the leader (Figure 2).
     * </p>
     */
    public void testMergeElectsLeaderWhenCoordinatorTrustsSteppedDownLeader(Class<?> ignore) throws Exception {
        withClusterSize(5);
        createCluster();
        long id = 0;

        // Node 2 coordinates the first view and wins the tie among empty logs.
        cluster.handleView(createView(id++, 2, 0, 1, 3, 4));
        BaseRaftElectionTest.waitUntilAllHaveLeaderElected(rafts(), 10_000);
        assertThat(raft(2).isLeader()).isTrue();
        long staleTerm = raft(2).currentTerm();

        // Node 4 splits away; the leader keeps a majority that includes node 0.
        cluster.handleView(createView(id++, 2, 0, 1, 3));
        cluster.handleView(createView(id++, 4));

        // Node 0 misses every view from here on. The leader drops to a minority and steps down.
        cluster.handleView(createView(id++, 2, 3));
        cluster.handleView(createView(id++, 1));
        assertThat(raft(2).isLeader()).isFalse();
        assertThat(raft(0).leader()).isEqualTo(address(2));

        // The partitions heal with node 0 as coordinator, still trusting the stepped-down leader.
        cluster.handleView(createView(id++, 0, 1, 2, 3, 4));

        assertNewLeaderElectedAfter(staleTerm);
    }

    /**
     * A merged cluster elects a leader when its coordinator trusts a stepped-down leader, even if the coordinator is
     * the same before and after the merge.
     * <p>
     * The leader is not the coordinator after a leadership transfer. Nothing in the merged view changes from the
     * coordinator's perspective, so only the members that saw the leader step down can reveal it.
     * </p>
     */
    public void testMergeElectsLeaderWhenSameCoordinatorTrustsSteppedDownLeader(Class<?> ignore) throws Exception {
        withClusterSize(5);
        createCluster();
        long id = 0;

        // Node 0 coordinates every view. It wins the first election and then hands the leadership to node 1.
        cluster.handleView(createView(id++, 0, 1, 2, 3, 4));
        BaseRaftElectionTest.waitUntilAllHaveLeaderElected(rafts(), 10_000);
        Address transferred = election(0).startForcedElection(address(0))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(transferred).isEqualTo(address(1));
        assertThat(RaftTestUtils.eventually(() -> Arrays.stream(rafts()).allMatch(r -> Objects.equals(r.leader(), address(1))), 10, TimeUnit.SECONDS)).isTrue();
        long staleTerm = raft(1).currentTerm();

        // Node 4 splits away; the leader keeps a majority with the coordinator.
        cluster.handleView(createView(id++, 0, 1, 2, 3));
        cluster.handleView(createView(id++, 4));

        // Node 0 misses every view from here on. The leader drops to a minority and steps down.
        cluster.handleView(createView(id++, 1, 2));
        cluster.handleView(createView(id++, 3));
        assertThat(raft(1).isLeader()).isFalse();
        assertThat(raft(0).leader()).isEqualTo(address(1));

        // The partitions heal under the same coordinator, which still trusts the stepped-down leader.
        cluster.handleView(createView(id++, 0, 1, 2, 3, 4));

        assertNewLeaderElectedAfter(staleTerm);
    }

    /**
     * Asserts every member agrees on a single leader elected after the given term.
     *
     * @param staleTerm the term of the leader that stepped down.
     */
    private void assertNewLeaderElectedAfter(long staleTerm) {
        BaseRaftElectionTest.waitUntilAllHaveLeaderElected(rafts(), 10_000);
        LOGGER.info("-- state after merge%n{}", dumpLeaderAndTerms());
        assertThat(leaders()).hasSize(1);
        assertThat(rafts()).allSatisfy(r -> assertThat(r.currentTerm()).isGreaterThan(staleTerm));
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
