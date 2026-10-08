package org.jgroups.protocols.raft.election;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.DummyStateMachine;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class LateJoinerRetrievesLeaderTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    private static final byte[] raft_id_key = Util.stringToBytes("raft-id");

    {
        createManually = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }

    /**
     * Tests that a late-joining node can retrieve the current leader when its ELECTION protocol
     * is installed after the cluster has already elected a leader.
     * <p>
     * <b>Scenario:</b>
     * <ol>
     *   <li>Start 2 nodes and elect a leader</li>
     *   <li>Install view with 3 nodes (simulating JChannel.connect() for node 3)</li>
     *   <li>Add the 3rd node's RAFT/ELECTION protocols to the cluster (simulating late FORK installation)</li>
     *   <li>Verify that node 3 discovers the leader via RetrieveLeaderElected request</li>
     * </ol>
     */
    public void testLateJoinerRetrievesLeader(Class<?> ignore) throws Exception {
        long viewId = 0;

        // Start with 2 nodes initially
        withClusterSize(2);
        createCluster();

        // Install initial view and elect leader
        LOGGER.info("-- installing initial 2-node view");
        View v1 = createView(viewId++, 0, 1);
        cluster.handleView(v1);

        // Wait for leader election
        LOGGER.info("-- waiting for leader election");
        waitUntilLeaderElected(10_000, 0, 1);
        Address leader = raft(0).leader();
        long term = raft(0).currentTerm();

        assertThat(leader)
                .withFailMessage("Leader should be elected between nodes 0 and 1")
                .isNotNull();

        LOGGER.info("-- leader elected: {} with term {}", leader, term);

        // Create the address for node 2 (simulating it joining at JGroups level)
        String name = "C";
        Address node2Address = createAddressForNode(name);

        // Install view with 3 nodes BEFORE creating the RAFT/ELECTION protocols
        // This simulates: JChannel.connect() happens, view includes node 2,
        // but FORK channel with ELECTION/RAFT doesn't exist yet
        LOGGER.info("-- installing view with 3rd node (FORK channel not yet created)");
        View v2 = View.create(address(0), viewId++, address(0), address(1), node2Address);
        cluster.handleView(v2);

        // Now create node 2's RAFT/ELECTION protocols (simulating late FORK channel creation)
        LOGGER.info("-- creating node 2's RAFT/ELECTION protocols (late FORK channel installation)");
        RaftNode node2 = createLateJoiningNode(name, node2Address);

        // Add to cluster and start
        cluster.add(node2Address, node2);
        node2.init();
        node2.start();

        // Deliver the view to the new node
        node2.handleView(v2);

        // The late joiner should discover the leader via RetrieveLeaderElected
        LOGGER.info("-- waiting for node 2 to discover the leader");
        assertThat(eventually(() -> {
            Address node2Leader = node2.raft().leader();
            long node2Term = node2.raft().currentTerm();
            LOGGER.trace("node 2: leader={}, term={} (expected leader={}, term={}})",
                    node2Leader, node2Term, leader, term);
            return node2Leader != null && node2Leader.equals(leader) && node2Term == term;
        }, 10, SECONDS))
                .withFailMessage(() -> String.format(
                        "Node 2 should discover leader %s with term %d, but saw leader=%s, term=%d",
                        leader, term, node2.raft().leader(), node2.raft().currentTerm()))
                .isTrue();
    }

    /**
     * Tests that a late joiner handles the case where no leader exists yet.
     * <p>
     * <b>Scenario:</b>
     * <ol>
     *   <li>Start a cluster with minimum nodes (no majority, no leader)</li>
     *   <li>Add a late joiner when still no leader exists</li>
     *   <li>Verify the late joiner gracefully handles the null leader response</li>
     *   <li>Add more nodes to reach majority and verify election proceeds normally</li>
     * </ol>
     */
    public void testLateJoinerWhenNoLeaderExists(Class<?> clazz) throws Exception {
        if (clazz == ELECTION2.class) throw new SkipException("Not running test for " + clazz);
        long viewId = 0;

        // Start with only 1 node (no majority, no leader possible)
        withClusterSize(5);
        createCluster(2);

        LOGGER.info("-- installing single-node view (no majority)");
        View v1 = createView(viewId++, 0, 1);
        cluster.handleView(v1);

        assertThat(raft(0).leader())
                .withFailMessage("Node 0 should not have a leader (no majority)")
                .isNull();

        // Now add node 2 to reach majority
        String name2 = "C";
        Address node2Address = createAddressForNode(name2);

        LOGGER.info("-- adding 3rd node to reach majority");
        View v3 = View.create(address(0), viewId++, address(0), address(1), node2Address);
        cluster.handleView(v3);

        RaftNode node2 = createLateJoiningNode(name2, node2Address);
        cluster.add(node2Address, node2);
        node2.init();
        node2.start();
        node2.handleView(v3);

        // All nodes should now elect a leader
        LOGGER.info("-- waiting for leader election with all 3 nodes");
        assertThat(eventually(() -> {
            Address leader0 = raft(0).leader();
            Address leader1 = raft(1).leader();
            Address leader2 = node2.raft().leader();

            return leader0 != null && leader0.equals(leader1) && leader0.equals(leader2);
        }, 10, SECONDS))
                .withFailMessage(() -> String.format(
                        "All nodes should agree on leader. Node 0: %s, Node 1: %s, Node 2: %s",
                        raft(0).leader(), raft(1).leader(), node2.raft().leader()))
                .isTrue();
    }

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.synchronous(true).stateMachine(new DummyStateMachine());
    }

    /**
     * Creates a RaftNode for late joining (after view is installed).
     */
    private RaftNode createLateJoiningNode(String name, Address address) throws Exception {
        Protocol[] stack = baseProtocolStackForNode(name);

        RAFT raft = null;
        BaseElection election = null;

        for (Protocol p : stack) {
            if (p instanceof RAFT) {
                raft = (RAFT) p;
            } else if (p instanceof BaseElection) {
                election = (BaseElection) p;
            }
        }

        assert raft != null : "RAFT not found in stack";
        raft.setAddress(address);

        if (election != null) {
            election.raft(raft).setAddress(address);
        }

        return new RaftNode(cluster, stack);
    }

    /**
     * Creates an address for a node (mirrors the internal createAddress in BaseRaftClusterTest).
     */
    private Address createAddressForNode(String name) {
        ExtendedUUID.setPrintFunction(RAFT.print_function);
        return ExtendedUUID.randomUUID(name).put(raft_id_key, Util.stringToBytes(name));
    }
}
