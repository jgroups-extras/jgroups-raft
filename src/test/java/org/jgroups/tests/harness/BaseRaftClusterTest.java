package org.jgroups.tests.harness;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.MockRaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Base class test utilizing {@link MockRaftCluster} instances to mock a cluster.
 *
 * <p>
 * This class implements the base utilities to retrieve the cluster members ({@link RaftNode}), access the cluster,
 * and the stack protocols.
 * </p>
 *
 * @param <T>: The type of the cluster instances.
 * @since 1.0.13
 * @see AbstractRaftTest
 * @see MockRaftCluster
 */
public abstract class BaseRaftClusterTest<T extends MockRaftCluster> extends AbstractRaftTest {

    /**
     * Keep track of the cluster members.
     */
    private RaftNode[] nodes;

    /**
     * The actual cluster instance. Visible to subclasses to interact.
     */
    protected T cluster;

    /**
     * Creates a new {@link MockRaftCluster} instance.
     *
     * @return A new {@link MockRaftCluster} instance. In case <code>null</code> is returned, the cluster is not created.
     */
    protected abstract T createNewMockCluster();

    @Override
    protected final void createCluster(int limit) throws Exception {
        System.out.printf("-- Creating mock cluster %s with size %s limited to %d%n", clusterName(), clusterSize, limit);

        if (cluster == null) {
            if ((cluster = createNewMockCluster()) == null) {
                System.out.println("-- No cluster instance created!");
                return;
            }
        }

        // Maybe resizing would be necessary when dynamically expanding the cluster.
        if (nodes == null) nodes = new RaftNode[clusterSize];

        while (limit > 0 && cluster.size() < clusterSize) {
            String name = Character.toString('A' + cluster.size());
            Address address = createAddress(name);

            RaftNode node = createNode(name, address);
            node.init();
            nodes[cluster.size()] = node;
            cluster.add(address, node);
            node.start();

            limit -= 1;
        }
    }

    @Override
    protected final void destroyCluster() throws Exception {
        beforeClusterDestroy();

        System.out.printf("-- Destroying mock cluster %s named %s with size %d%n", cluster, clusterName(), clusterSize);

        if (nodes == null) return;

        for (int i = clusterSize - 1; i >= 0; i--) {
            close(i);
        }

        cluster.clear();
        cluster = null;
        nodes = null;

        afterClusterDestroy();
    }

    /**
     * Close the member identified by the index.
     * <p>
     * This method removes the {@link RaftNode} from the active member list, subsequent calls to {@link #node(int)}
     * will return <code>null</code>. Also removes the member from the actual cluster view and deletes the
     * {@link RAFT} persisted state.
     * </p>
     *
     * @param index: The index to retrieve the {@link RaftNode}.
     * @throws Exception: If an error happens while clearing the resources.
     * @see BaseRaftChannelTest#close(int)
     */
    protected final void close(int index) throws Exception {
        RaftNode node = node(index);

        if (node == null) return;

        cluster.remove(node.getAddress());

        node.stop();
        node.destroy();

        // Always stop everything **before** deleting the log.
        RaftTestUtils.deleteRaftLog(node.raft());

        nodes[index] = null;
    }

    @Override
    protected Protocol[] baseProtocolStackForNode(String name) throws Exception {
        return new Protocol[] {
                createNewElectionAndDecorate(),
                createNewRaft(name),
        };
    }

    /**
     * Retrieves a {@link RaftNode} by index.
     *
     * @param index: Index to retrieve the member.
     * @return A {@link RaftNode} instance or <code>null</code>.
     * @throws AssertionError: in case the {@param index} is greater that the {@link #clusterSize} or the cluster
     *                         was not created.
     */
    protected final RaftNode node(int index) {
        assert index < clusterSize : "Index out of bounds, maximum " + clusterSize;
        assert nodes != null : "Cluster not created!";
        return nodes[index];
    }

    /**
     * Retrieves the {@link RAFT} instance from the node identified by the index.
     *
     * @param index: Index to retrieve the cluster member.
     * @return A {@link RAFT} instance of <code>null</code> in case the member is not present.
     * @see #node(int)
     */
    protected final RAFT raft(int index) {
        RaftNode node = node(index);
        return node == null ? null : node.raft();
    }

    /**
     * Retrieves the {@link Address} of the member identified by the index.
     *
     * @param index: Index to retrieve the cluster member.
     * @return The member's {@link Address} or <code>null</code> if it is not present in the cluster.
     */
    protected final Address address(int index) {
        RaftNode node = node(index);
        return node.raft().getAddress();
    }

    /**
     * Retrieve all cluster members.
     *
     * <p>
     * The array might contain <code>null</code> entries.
     * </p>
     *
     * @return All the cluster {@link RaftNode}s.
     */
    protected final RaftNode[] nodes() {
        return nodes;
    }

    /**
     * Retrieve all the {@link RAFT}s in the cluster.
     *
     * @return The existing {@link RAFT} instances. There is no <code>null</code> entries.
     */
    protected final RAFT[] rafts() {
        return Arrays.stream(nodes)
                .filter(Objects::nonNull)
                .map(RaftNode::raft)
                .toArray(RAFT[]::new);
    }

    /**
     * Generates a message identifying the term and leader of each cluster member.
     *
     * @return A string containing a cluster-wide view of leader and term.
     */
    protected String dumpLeaderAndTerms() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes.length; i++) {
            RaftNode node = nodes[i];

            if (node == null) {
                sb.append(Character.toString('A' + i))
                        .append(" is null")
                        .append(System.lineSeparator());
                continue;
            }

            RAFT raft = node.raft();
            sb.append(raft.raftId())
                    .append(": (")
                    .append(raft.currentTerm())
                    .append(") with leader ")
                    .append(raft.leader())
                    .append(System.lineSeparator());
        }
        return sb.toString();
    }

    /**
     * Retrieves the {@link RAFT} leader of each active node in the cluster.
     * <p>
     * This method also removes the <code>null</code> entries.
     * </p>
     *
     * @return Non-null leader of each cluster member.
     */
    protected final List<Address> leaders() {
        return Arrays.stream(nodes)
                .filter(Objects::nonNull)
                .map(RaftNode::raft)
                .map(RAFT::leader)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Creates a new {@link View}.
     * <p>
     * The method creates a new view containing all the active members identified by the indexes. If a member for
     * a given index is not present in the cluster, it is not added to the final view.
     * </p>
     *
     * <p>
     * The index on the first entry, i.e., <code>indexes[0]</code>, is the view coordinator.
     * </p>
     *
     * @param id: The {@link View} id.
     * @param indexes: The cluster members to add to the view.
     * @return A new {@link View} instance.
     * @throws AssertionError if no members identified by the indexes is present in the cluster.
     * @see #node(int)
     */
    protected final View createView(long id, int ... indexes) {
        List<Address> addresses = Arrays.stream(indexes)
                .distinct()
                .mapToObj(this::node)
                .filter(Objects::nonNull)
                .map(RaftNode::getAddress)
                .collect(Collectors.toList());

        assert !addresses.isEmpty() : "No members left to create a view!";
        return View.create(addresses.get(0), id, addresses);
    }

    private RaftNode createNode(String name, Address address) throws Exception {
        Protocol[] stack = baseProtocolStackForNode(name);
        applyTraceConfiguration(stack);

        RAFT r = findProtocol(stack, RAFT.class);
        BaseElection be = findProtocol(stack, BaseElection.class);

        assert r != null : "RAFT never found!";
        r.setAddress(address);

        // Some tests use cluster without election.
        if (be != null) be.raft(r).setAddress(address);

        return new RaftNode(cluster, stack);
    }

    private Address createAddress(String name) {
        ExtendedUUID.setPrintFunction(RAFT.print_function);
        return ExtendedUUID.randomUUID(name).put(RAFT.raft_id_key, Util.stringToBytes(name));
    }

    @SuppressWarnings("unchecked")
    private <P extends Protocol> P findProtocol(Protocol[] stack, Class<P> clazz) {
        for (Protocol protocol : stack) {
            if (clazz.isAssignableFrom(protocol.getClass()))
                return (P) protocol;
        }
        return null;
    }
}
