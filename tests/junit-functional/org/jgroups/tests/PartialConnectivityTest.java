package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.PartitionedRaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.util.Utils;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Test(groups= Global.FUNCTIONAL, singleThreaded=true)
public class PartialConnectivityTest {

    private static final String CLUSTER = PartialConnectivityTest.class.getSimpleName();
    protected final PartitionedRaftCluster cluster = new PartitionedRaftCluster();
    protected final List<String> members= Arrays.asList("A", "B", "C", "D", "E");
    protected RaftNode[] nodes;
    protected RaftNode a, b, c, d, e;
    protected RAFT[] rafts;
    protected Class<? extends BaseElection> electionClass;

    public PartialConnectivityTest() { }

    public PartialConnectivityTest(Class<? extends BaseElection> electionClass) {
        this.electionClass = electionClass;
    }

    @BeforeMethod
    protected void init() throws Exception {
        nodes = new RaftNode[5];
        rafts = new RAFT[5];

        int i=0;
        a = createNode("A", i++);
        b = createNode("B", i++);
        c = createNode("C", i++);
        d = createNode("D", i++);
        e = createNode("E", i);
    }

    @AfterMethod
    protected void destroy() throws Exception {
        for (int i=nodes.length - 1; i >= 0; i--) {
            nodes[i].stop();
            nodes[i].destroy();
            Utils.deleteLog(rafts[i]);
        }
        Util.close(a,b,c,d,e);
        cluster.clear();
    }

    @Factory
    protected static Object[] electionClass() {
        return new Object[] {
                new PartialConnectivityTest(ELECTION.class),
                new PartialConnectivityTest(ELECTION2.class),
        };
    }

    public void testQuorumLossAndRecovery() {
        long id=1;

        View initial=createView(id++, a,b,c,d,e);
        cluster.handleView(initial);

        assert Util.waitUntilTrue(5000, 200, () -> Stream.of(nodes).allMatch(n -> n.raft().leader() != null));
        List<Address> leaders = leaders(nodes);
        assert leaders.size() == 1 : "there should be only one leader, but found " + leaders;
        Address leader = leaders.get(0);

        System.out.println("leader is " + leader);

        // Nodes D and E do not update their view.
        cluster.handleView(createView(id++, a, c));
        cluster.handleView(createView(id++, b, c));

        assert Util.waitUntilTrue(3000, 200, () -> Stream.of(a, b, c).allMatch(n -> n.raft().leader() == null));

        assert leader.equals(d.raft().leader()) : "leader should be " + leader + ", but found " + d.raft().leader();
        assert leader.equals(e.raft().leader()) : "leader should be " + leader + ", but found " + e.raft().leader();

        for (RaftNode n : nodes) {
            assert !n.election().isVotingThreadRunning() : "election thread should not be running in " + n;
        }

        View after = createView(id++, e, d, a, b, c);
        System.out.println("after restored network: " + after);
        cluster.handleView(after);

        boolean elected = Util.waitUntilTrue(3000, 200, () -> Stream.of(nodes).allMatch(n -> n.raft().leader() != null));
        if (electionClass.equals(ELECTION2.class)) {
            assert elected : "leader was never elected again";
            leaders = leaders(nodes);
            assert leaders.size() == 1 : "there should be only one leader, but found " + leaders;
            System.out.println("Leader after restored network: " + leaders.get(0));
        } else {
            assert !elected : "leader was elected again";
            assert electionClass.equals(ELECTION.class);
        }
    }

    protected static View createView(long id, RaftNode... mbrs) {
        List<Address> l=Stream.of(mbrs).filter(Objects::nonNull).map(RaftNode::getAddress).collect(Collectors.toList());
        return View.create(l.get(0), id, l);
    }

    protected static Address createAddress(String name) {
        ExtendedUUID.setPrintFunction(RAFT.print_function);
        return ExtendedUUID.randomUUID(name).put(RAFT.raft_id_key, Util.stringToBytes(name));
    }

    protected RaftNode createNode(String name, int i) throws Exception {
        Address a=createAddress(name);
        RAFT r=rafts[i]=new RAFT().members(members).raftId(name)
                .logClass(InMemoryLog.class.getCanonicalName())
                .logPrefix(name + "-" + CLUSTER)
                .stateMachine(new DummyStateMachine())
                .synchronous(true)
                .setAddress(a);
        BaseElection e=election(r, a);
        RaftNode node=nodes[i]=new RaftNode(cluster, new Protocol[]{e, r});
        node.init();
        node.start();
        cluster.add(a, node);
        return node;
    }

    private BaseElection election(RAFT r, Address a) throws Exception {
        return electionClass.getConstructor().newInstance().raft(r).setAddress(a);
    }

    public static List<Address> leaders(RaftNode... nodes) {
        List<Address> leaders = new ArrayList<>();
        for (RaftNode node : nodes) {
            if (node.raft().isLeader()) {
                leaders.add(node.getAddress());
            }
        }
        return leaders;
    }
}
