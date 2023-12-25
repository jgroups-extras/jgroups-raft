package org.jgroups.tests.harness;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * The base class for tests that utilize {@link JChannel} for communication.
 * <p>
 * This class already provides the mechanism to retrieve and access the protocols in the {@link JChannel}. Creating a
 * cluster waits until all the nodes receive the same view. The cluster can be resized dynamically during a test
 * execution, growing to accommodate more nodes, but not shrinking.
 * </p>
 */
public class BaseRaftChannelTest extends AbstractRaftTest {

    private JChannel[] channels;

    @Override
    protected final void createCluster(int limit) throws Exception {
        System.out.printf("%s: creating cluster '%s' with size %d (limited %d) %n", getClass(), clusterName(), clusterSize, limit);

        if (this.channels == null) this.channels = new JChannel[clusterSize];
        if (this.channels.length < clusterSize) {
            JChannel[] updated = new JChannel[clusterSize];
            System.arraycopy(channels, 0, updated, 0, channels.length);
            channels = updated;
        }

        for (int i = 0; i < clusterSize; i++) {

            // The creation can be invoked multiple times to resize the cluster and add new members.
            if (channels[i] != null) continue;

            String name = Character.toString('A' + i);
            channels[i] = createChannel(name);
            limit -= 1;

            if (limit == 0) break;
        }

        Util.waitUntilAllChannelsHaveSameView(10_000, 100, actualChannels());
        System.out.printf("%s: current cluster %s of size %d view is:%n%s%n", getClass(), clusterName(), clusterSize, printCurrentCluster());
    }

    /**
     * Creates a new {@link JChannel} using the configurations and attached to the lifecycle entrypoint.
     *
     * @param name: The {@link JChannel} name.
     * @return A new instance of the {@link JChannel}.
     * @throws Exception: If failed to create the channel.
     */
    protected JChannel createChannel(String name) throws Exception {
        JChannel ch = createDisconnectedChannel(name);
        beforeChannelConnection(ch);
        ch.connect(clusterName());
        afterChannelConnection(ch);
        return ch;
    }

    /**
     * Creates a disconnected {@link JChannel}.
     *
     * @param name: The {@link JChannel} name.
     * @return A disconnected new instance of the {@link JChannel}.
     * @throws Exception: If failed to create the channel.
     */
    protected JChannel createDisconnectedChannel(String name) throws Exception {
        Protocol[] stack = baseProtocolStackForNode(name);

        applyTraceConfiguration(stack);

        JChannel ch = new JChannel(stack);
        ch.name(name);
        return ch;
    }

    /**
     * Destroy the complete cluster.
     * <p>
     * This iterates the {@link #channels} in reverse order to avoid any major disruption. Each channel is closed and
     * set to null. This deletes the state information of the {@link RAFT} instance, deleting the log. All the
     * created resources are cleared after this method executes.
     * </p>
     *
     * @throws Exception: If an error happens while clearing the resources.
     */
    @Override
    protected final void destroyCluster() throws Exception {
        beforeClusterDestroy();

        System.out.printf("%s: destroying cluster %s with size %d%n", getClass(), clusterName(), clusterSize);
        if (channels != null) {
            for (int i = clusterSize - 1; i >= 0; i--) {
                close(i);
            }
        }

        this.channels = null;
        afterClusterDestroy();
    }

    /**
     * Close the {@link JChannel} in the given index.
     * <p>
     * The {@link JChannel} is closed and the {@link RAFT} state is deleted.
     * </p>
     *
     * @param index: The channel to close.
     * @throws Exception: If an error happens while closing the resources.
     */
    protected final void close(int index) throws Exception {
        JChannel ch = channel(index);
        if (ch == null) return;

        channels[index] = null;
        close(ch);
    }

    /**
     * Closes the given channel and deletes the {@link RAFT} state.
     *
     * @param ch: The {@link JChannel} to close.
     * @throws Exception: If an error happens while closing the resources.
     */
    protected final void close(JChannel ch) throws Exception {
        if (ch == null) return;

        if (channels != null) {
            int i = 0;
            for (JChannel curr : channels) {
                if (curr == ch) {
                    channels[i] = null;
                    break;
                }
                i++;
            }
        }

        // Always close the channel before deleting the log.
        Util.close(ch);
        RaftTestUtils.deleteRaftLog(raft(ch));
    }

    /**
     * The stack to utilize in the node during the test with a channel.
     * <p>
     * By default, besides decorating with the protocols from {@link Util#getTestStack(Protocol...)}, we use:
     * <ol>
     *     <li>An election algorithm {@link BaseElection}.</li>
     *     <li>The {@link RAFT} protocol.</li>
     *     <li>The {@link REDIRECT} protocol.</li>
     * </ol>
     * </p>
     *
     * @param name: The node's name creating the stack.
     * @return The complete stack in the correct order.
     */
    @Override
    protected Protocol[] baseProtocolStackForNode(String name) throws Exception {
        return Util.getTestStack(createNewElectionAndDecorate(), createNewRaft(name), new REDIRECT());
    }

    /**
     * Get the created channel in the provided index.
     *
     * @param index: The index of the channel to retrieve.
     * @return The channel in the specified position. Might return <code>null</code> if the channel was closed.
     */
    protected final JChannel channel(int index) {
        assert index < channels.length : "Index out of bounds, maximum is " + (clusterSize - 1);
        return channels[index];
    }

    /**
     * Get the {@link JChannel} with the given address.
     *
     * @param addr: Channel address to match.
     * @return The channel with the given address, or <code>null</code>, otherwise.
     */
    protected final JChannel channel(Address addr) {
        for(JChannel ch: channels) {
            if(ch.getAddress() != null && ch.getAddress().equals(addr))
                return ch;
        }
        return null;
    }

    /**
     * Get the created {@link RAFT} in the provided index.
     *
     * @param index: The index of the RAFT instance to retrieve.
     * @return The {@link RAFT} in the specified position or <code>null</code>.
     * @see #channel(int)
     */
    protected final RAFT raft(int index) {
        JChannel ch = channel(index);
        return ch == null ? null : raft(ch);
    }

    /**
     * The {@link RAFT} instance in the provided {@link JChannel}.
     *
     * @param ch: The channel to retrieve the protocol.
     * @return The RAFT protocol instance or <code>null</code>.
     */
    protected final RAFT raft(JChannel ch) {
        return RaftTestUtils.raft(ch);
    }

    /**
     * Search the first node identified as leader.
     * As per the Raft specification, the algorithm can have more than one leader in different terms. If this method
     * is invoked during a network split or during an election round, it could have fuzzy results. It is better suited
     * to use when a stable topology is in place.
     *
     * @return The first node identified as an elected leader.
     */
    protected final RAFT leader() {
        for (JChannel ch : channels) {
            if (ch == null) continue;

            RAFT r = raft(ch);
            if (r.isLeader()) return r;
        }

        return null;
    }

    /**
     * Finds the first node identified as leader and retrieves the address.
     *
     * @return The leader address or <code>null</code>.
     */
    protected final Address leaderAddress() {
        RAFT r = leader();
        return r == null ? null : r.getAddress();
    }

    /**
     * Iterate over all nodes and return the ones which identify itself as leader.
     *
     * @return All nodes which sees itself as leader.
     * @see #leader()
     */
    protected final List<RAFT> leaders() {
        List<RAFT> leaders = new ArrayList<>();
        for (JChannel ch : channels) {
            if (ch == null) continue;

            RAFT r = raft(ch);
            if (r.isLeader()) leaders.add(r);
        }

        return leaders;
    }

    /**
     * Generates a message with all members view of the current term and leader.
     *
     * @return A string with a cluster-wide view of the cluster.
     */
    protected final String dumpLeaderAndTerms() {
        List<String> members = new ArrayList<>(getRaftMembers());
        StringBuilder builder = new StringBuilder("Cluster: ")
                .append(clusterName())
                .append(System.lineSeparator());

        for (int i = 0; i < clusterSize; i++) {
            String member = members.get(i);
            RAFT raft = raft(i);

            if (raft == null) {
                builder.append(member)
                        .append(" is null");
            } else {
                builder.append(member)
                        .append(" -> ")
                        .append(raft.currentTerm())
                        .append(" and leader ")
                        .append(raft.leader());
            }

            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }

    protected final String printCurrentCluster() {
        List<String> members = new ArrayList<>(getRaftMembers());

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < clusterSize; i++) {
            JChannel ch = channel(i);
            if (ch == null) {
                sb.append(members.get(i)).append(" is null");
            } else {
                sb.append(members.get(i))
                        .append(": ")
                        .append(ch.getViewAsString());
            }
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }

    /**
     * Get the complete array of channels.
     * During the execution, as channels are closed, the array might contain <code>null</code> entries.
     *
     * @return The complete array of channels.
     */
    protected final JChannel[] channels() {
        return channels;
    }

    /**
     * Iterate over all channels and remove any <code>null</code> entries.
     *
     * @return The array of currently active channels.
     */
    protected final JChannel[] actualChannels() {
        List<JChannel> c = new ArrayList<>(channels.length);
        for (JChannel ch : channels) {
            if (ch != null) c.add(ch);
        }

        return c.toArray(new JChannel[0]);
    }

    /**
     * Retrieve the address of the channel by index.
     *
     * @param index: The channel index to retrieve the address.
     * @return The channel address.
     */
    protected final Address address(int index) {
        return channel(index).getAddress();
    }

    /**
     * Entrypoint executed before the {@link JChannel} connects to the cluster.
     * This method is invoked for every new {@link JChannel} instance created. The channel is already configured at
     * this point and contains the configured protocol stack.
     *
     * @param ch: The configured channel which is connecting.
     * @throws Exception: If an exception happens during the execution.
     */
    protected void beforeChannelConnection(JChannel ch) throws Exception { }

    /**
     * Entrypoint executed after the {@link JChannel} connects to the cluster.
     *
     * @param ch: The channel connected instance.
     * @throws Exception: If an exception happens during the execution.
     * @see #beforeChannelConnection(JChannel)
     */
    protected void afterChannelConnection(JChannel ch) throws Exception { }
}
