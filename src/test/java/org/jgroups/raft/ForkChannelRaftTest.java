package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.FORK;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies Raft runs correctly when its stack is installed at runtime on a {@link ForkChannel}.
 *
 * <p>
 * Mirrors how Infinispan uses Raft: the main channels form the cluster first, and each node later adds the Raft stack
 * (ELECTION, RAFT, REDIRECT) on a fork channel. Messages sent to a node before its fork stack exists are dropped by
 * {@link FORK}, so the nodes must still converge on a leader without them.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ForkChannelRaftTest {

    private static final byte[] RAFT_ID_KEY = Util.stringToBytes("raft-id");
    private static final String FORK_ID = "raft-fork";
    private static final List<String> MEMBERS = List.of("A", "B", "C");
    private static final long VOTE_TIMEOUT_MS = 50;

    private final List<JChannel> mainChannels = new ArrayList<>();
    private final List<RaftForkNode> nodes = new ArrayList<>();
    private String clusterName;

    @BeforeMethod
    protected void beforeMethod() {
        clusterName = getClass().getSimpleName() + "-" + System.nanoTime();
    }

    @AfterMethod(alwaysRun = true)
    protected void afterMethod() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            nodes.get(i).stop();
        }
        nodes.clear();

        for (int i = mainChannels.size() - 1; i >= 0; i--) {
            Util.close(mainChannels.get(i));
        }
        mainChannels.clear();

        // InMemoryLog keeps logs in a static map so restarts in the same JVM reload them; drop this test's logs.
        InMemoryLog.logs.keySet().removeIf(name -> name.startsWith(clusterName));
    }

    /**
     * The view coordinator is the last node to install the Raft stack.
     * <p>
     * Only the coordinator starts elections. Until it installs the stack, nobody can elect a leader, and once it does,
     * connecting the fork channel must be enough for it to start the election.
     * </p>
     */
    public void testCoordinatorInstallsRaftLast() throws Exception {
        JChannel a = connectMainChannel("A");
        JChannel b = connectMainChannel("B");
        JChannel c = connectMainChannel("C");

        assertThat(a.getView().getCoord()).isEqualTo(a.getAddress());

        RaftForkNode nodeB = installRaft(b);
        RaftForkNode nodeC = installRaft(c);
        RaftForkNode nodeA = installRaft(a);

        assertLeaderAgreement(nodeA, nodeB, nodeC);
        assertFollowerWriteVisibleEverywhere(nodeA, nodeB, nodeC);
    }

    /**
     * A node installs the Raft stack after the others already elected a leader.
     * <p>
     * The leader announced itself while the late node had no fork stack, so FORK dropped the announcement. The late
     * node must still learn the leader without a new election or a membership change.
     * </p>
     */
    public void testLateInstallLearnsElectedLeader() throws Exception {
        JChannel a = connectMainChannel("A");
        JChannel b = connectMainChannel("B");
        JChannel c = connectMainChannel("C");

        RaftForkNode nodeA = installRaft(a);
        RaftForkNode nodeB = installRaft(b);
        assertLeaderAgreement(nodeA, nodeB);
        long term = nodeA.raft.state().term();

        // No writes before C installs: the leader only resends AppendEntries to members missing entries, so an empty
        // log leaves C without any message from the leader that would reveal it.
        RaftForkNode nodeC = installRaft(c);

        assertLeaderAgreement(nodeA, nodeB, nodeC);
        assertThat(nodeC.raft.state().term())
                .withFailMessage(() -> "Late node should join the existing term without a new election: " + describe(nodeA, nodeB, nodeC))
                .isEqualTo(term);

        // C is a follower for sure here, so its write exercises the redirect to the leader it learned.
        String expected = "value-C";
        nodeC.raft.write(kv -> kv.handlePut("key", expected));
        for (RaftForkNode node : List.of(nodeA, nodeB, nodeC)) {
            assertThat(node.raft.<String>read(kv -> kv.handleGet("key")))
                    .withFailMessage("Node %s did not read the late node's write", node.raftId)
                    .isEqualTo(expected);
        }
    }

    /**
     * The membership changes after a node created its fork channel but before it connected it.
     * <p>
     * FORK delivers views to every fork stack, connected or not, so the node's election protocol sees a view before it
     * has an address. Connecting must still let the node learn the elected leader.
     * </p>
     */
    public void testViewChangeBeforeForkConnects() throws Exception {
        JChannel a = connectMainChannel("A");
        JChannel b = connectMainChannel("B");
        JChannel c = connectMainChannel("C");

        RaftForkNode nodeA = installRaft(a);
        RaftForkNode nodeB = installRaft(b);
        assertLeaderAgreement(nodeA, nodeB);
        long term = nodeA.raft.state().term();

        ForkChannel forkC = createRaftFork(c);

        // D is not a Raft member, it only changes the main channel's view while C's fork is still unconnected.
        connectMainChannel("D");
        assertThat(eventually(() -> c.getView().size() == 4, 10, TimeUnit.SECONDS))
                .withFailMessage(() -> "C did not install the view with D: " + c.getView())
                .isTrue();

        RaftForkNode nodeC = startRaft("C", forkC);

        assertLeaderAgreement(nodeA, nodeB, nodeC);
        assertThat(nodeC.raft.state().term())
                .withFailMessage(() -> "Late node should join the existing term without a new election: " + describe(nodeA, nodeB, nodeC))
                .isEqualTo(term);
    }

    /**
     * A follower stops its Raft stack and installs it again while its main channel stays in the cluster.
     * <p>
     * This is how Infinispan restarts a Raft cache on a node. Nobody sees a membership change, so the restarted
     * follower must learn the leader on its own.
     * </p>
     */
    public void testFollowerRestartsRaftStack() throws Exception {
        connectMainChannel("A");
        connectMainChannel("B");
        connectMainChannel("C");

        RaftForkNode[] cluster = {installRaft(mainChannel("A")), installRaft(mainChannel("B")), installRaft(mainChannel("C"))};
        assertLeaderAgreement(cluster);
        assertFollowerWriteVisibleEverywhere(cluster);
        long term = cluster[0].raft.state().term();

        int index = followerIndex(cluster);
        RaftForkNode follower = cluster[index];
        follower.stop();
        nodes.remove(follower);

        // The log survives the restart, so the follower misses no entries and the leader sends it nothing.
        cluster[index] = installRaft(mainChannel(follower.raftId));
        RaftForkNode restarted = cluster[index];

        assertLeaderAgreement(cluster);
        assertThat(restarted.raft.state().term())
                .withFailMessage(() -> "Restarted follower should rejoin the existing term without a new election: " + describe(cluster))
                .isEqualTo(term);

        String expected = "after-restart";
        restarted.raft.write(kv -> kv.handlePut("key", expected));
        for (RaftForkNode node : cluster) {
            assertThat(node.raft.<String>read(kv -> kv.handleGet("key")))
                    .withFailMessage("Node %s did not read the restarted follower's write", node.raftId)
                    .isEqualTo(expected);
        }
    }

    /**
     * The leader stops its Raft stack and installs it again while its main channel stays in the cluster.
     * <p>
     * The leader is not the view coordinator, so the coordinator still remembers the restarted node as the leader.
     * A restarted node must come back as a follower: the cluster runs a new election instead of handing the node its
     * previous leadership back. The new leader must then serve reads of entries committed in the previous term.
     * </p>
     */
    public void testLeaderRestartsRaftStack() throws Exception {
        connectMainChannel("A");
        connectMainChannel("B");
        connectMainChannel("C");

        RaftForkNode[] cluster = {installRaft(mainChannel("A")), installRaft(mainChannel("B")), installRaft(mainChannel("C"))};
        assertLeaderAgreement(cluster);

        // A is the coordinator and the first leader; move leadership away so the leader is not the coordinator.
        String leaderId = cluster[0].raft.administration().forceLeaderElection()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        assertThat(leaderId).isNotEqualTo("A");
        assertLeaderAgreement(cluster);
        long term = cluster[0].raft.state().term();

        // Committed before the leader restarts, read after the election in the next term.
        String before = "before-restart";
        cluster[0].raft.write(kv -> kv.handlePut("before", before));

        int index = indexOf(leaderId, cluster);
        RaftForkNode leader = cluster[index];
        leader.stop();
        nodes.remove(leader);

        cluster[index] = installRaft(mainChannel(leaderId));
        RaftForkNode restarted = cluster[index];

        // A new election means a new term, whichever node wins it.
        // Checked before agreement, since all nodes still agree on the restarted node right after it comes back.
        assertThat(eventually(() -> {
            for (RaftForkNode node : cluster) {
                if (node.raft.state().term() <= term)
                    return false;
            }
            return true;
        }, 10, TimeUnit.SECONDS))
                .withFailMessage(() -> String.format("Restarted leader should not resume term %d without an election: %s", term, describe(cluster)))
                .isTrue();
        assertLeaderAgreement(cluster);

        // The new leader must know the write of the previous term is committed before serving reads
        // (Raft dissertation, §6.4, step 1).
        for (RaftForkNode node : cluster) {
            assertThat(node.raft.<String>read(kv -> kv.handleGet("before")))
                    .withFailMessage("Node %s did not read the write committed before the restart", node.raftId)
                    .isEqualTo(before);
        }

        String after = "after-restart";
        restarted.raft.write(kv -> kv.handlePut("key", after));
        for (RaftForkNode node : cluster) {
            assertThat(node.raft.<String>read(kv -> kv.handleGet("key")))
                    .withFailMessage("Node %s did not read the restarted leader's write", node.raftId)
                    .isEqualTo(after);
        }
    }

    private static int indexOf(String raftId, RaftForkNode ... nodes) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].raftId.equals(raftId))
                return i;
        }
        throw new IllegalArgumentException("No node " + raftId + ": " + describe(nodes));
    }

    private JChannel mainChannel(String name) {
        for (JChannel ch : mainChannels) {
            if (name.equals(ch.getName()))
                return ch;
        }
        throw new IllegalArgumentException("No main channel named " + name);
    }

    private static int followerIndex(RaftForkNode ... nodes) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].raft.role() != JGroupsRaftRole.LEADER)
                return i;
        }
        throw new AssertionError("No follower found: " + describe(nodes));
    }

    private JChannel connectMainChannel(String name) throws Exception {
        JChannel ch = new JChannel(
                new SHARED_LOOPBACK(),
                new SHARED_LOOPBACK_PING(),
                new NAKACK2(),
                new UNICAST3(),
                new STABLE(),
                new GMS().setJoinTimeout(1000),
                new FORK()
        ).name(name);

        // The fork stack shares the main channel's address, so the raft-id must be in it before connecting.
        // Infinispan does the same in JGroupsTransport.
        ch.addAddressGenerator(() -> ExtendedUUID.randomUUID(name).put(RAFT_ID_KEY, Util.stringToBytes(name)));
        ch.connect(clusterName);
        mainChannels.add(ch);
        return ch;
    }

    /**
     * Installs the Raft stack on a fork of the given main channel, the same way Infinispan does at runtime.
     */
    private RaftForkNode installRaft(JChannel main) throws Exception {
        return startRaft(main.getName(), createRaftFork(main));
    }

    /**
     * Creates the fork channel with the Raft stack without connecting it.
     * <p>
     * The fork stack exists and receives the main channel's views from this point, but it has no address and its
     * protocols are not started until {@link #startRaft(String, ForkChannel)}.
     * </p>
     */
    private ForkChannel createRaftFork(JChannel main) throws Exception {
        String raftId = main.getName();
        RAFT raft = new RAFT()
                .members(MEMBERS)
                .raftId(raftId)
                .logClass(InMemoryLog.class.getCanonicalName())
                .logPrefix(clusterName + "-" + raftId);

        // A voting round waits for every Raft member, and a member without the fork stack never answers.
        // These tests elect with a member missing, so a short timeout keeps each round from waiting the default 600ms.
        BaseElection election = new ELECTION().voteTimeout(VOTE_TIMEOUT_MS);

        // Protocols are listed from bottom to top.
        return new ForkChannel(main, FORK_ID, FORK_ID, election, raft, new REDIRECT());
    }

    /**
     * Builds and starts the {@link JGroupsRaft} instance, which connects the fork channel.
     */
    private RaftForkNode startRaft(String raftId, ForkChannel fork) {
        JGroupsRaft<SimpleKVStateMachine> jgr = JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                .withJChannel(fork)
                .withClusterName(FORK_ID)
                .build();
        jgr.start();

        RaftForkNode node = new RaftForkNode(raftId, jgr, fork);
        nodes.add(node);
        return node;
    }

    private static void assertLeaderAgreement(RaftForkNode ... nodes) {
        assertThat(eventually(() -> {
            String leader = nodes[0].raft.state().leader();
            if (leader == null) return false;

            for (RaftForkNode node : nodes) {
                if (!Objects.equals(leader, node.raft.state().leader()))
                    return false;
            }
            return true;
        }, 10, TimeUnit.SECONDS))
                .withFailMessage(() -> "Nodes did not agree on a leader: " + describe(nodes))
                .isTrue();
    }

    private static void assertFollowerWriteVisibleEverywhere(RaftForkNode ... nodes) {
        RaftForkNode follower = null;
        for (RaftForkNode node : nodes) {
            if (node.raft.role() != JGroupsRaftRole.LEADER) {
                follower = node;
                break;
            }
        }
        assertThat(follower).withFailMessage("No follower found: %s", describe(nodes)).isNotNull();

        // A write from a follower goes through REDIRECT, so it only succeeds if the follower knows the leader.
        String expected = "value-" + follower.raftId;
        follower.raft.write(kv -> kv.handlePut("key", expected));

        for (RaftForkNode node : nodes) {
            assertThat(node.raft.<String>read(kv -> kv.handleGet("key")))
                    .withFailMessage("Node %s did not read the follower's write", node.raftId)
                    .isEqualTo(expected);
        }
    }

    private static String describe(RaftForkNode ... nodes) {
        StringBuilder sb = new StringBuilder();
        for (RaftForkNode node : nodes) {
            sb.append(node.raftId)
                    .append("(role=").append(node.raft.role())
                    .append(", leader=").append(node.raft.state().leader())
                    .append(", term=").append(node.raft.state().term())
                    .append(") ");
        }
        return sb.toString();
    }

    private record RaftForkNode(String raftId, JGroupsRaft<SimpleKVStateMachine> raft, ForkChannel fork) {

        /**
         * Stops Raft and closes the fork channel, so FORK discards the fork stack and a later install creates new protocols.
         */
        void stop() {
            raft.stop();
            Util.close(fork);
        }
    }
}
