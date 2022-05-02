package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.protocols.raft.*;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.util.Utils;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Uses the synchronous test framework to test {@link org.jgroups.protocols.raft.ELECTION}
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class SyncElectionTests {
    protected final Address      a,b,c;
    protected final Address[]    addrs={a=createAddress("A"), b=createAddress("B"), c=createAddress("C")};
    protected final List<String> mbrs=List.of("A", "B", "C");
    protected final RaftCluster  cluster=new RaftCluster();
    protected RAFT[]             rafts=new RAFT[3];
    protected ELECTION[]         elections=new ELECTION[3];
    protected RaftNode[]         nodes=new RaftNode[3];
    protected int                view_id=1;


    @BeforeMethod protected void init() {view_id=1;}

    @AfterMethod
    protected void destroy() throws Exception {
        for(int i=nodes.length-1; i >= 0; i--) {
            if(nodes[i] != null) {
                nodes[i].stop();
                nodes[i].destroy();
                nodes[i]=null;
            }
            if(rafts[i] != null) {
                Utils.deleteLogAndSnapshot(rafts[i]);
                rafts[i]=null;
            }
            if(elections[i] != null) {
                elections[i].stopVotingThread();
                elections[i]=null;
            }
        }
        cluster.clear();
    }

    /** Not really an election, as we only have a single member */
    public void testSingletonElection() throws Exception {
        createNode(0, "A");
        assert !rafts[0].isLeader();
        View view=createView();
        cluster.handleView(view);
        assert !rafts[0].isLeader();
        Util.waitUntilTrue(2000, 200, () -> rafts[0].isLeader());
        assert !rafts[0].isLeader();
        // assert !elections[0].isVotingThreadRunning();
        waitUntilVotingThreadHasStopped();
    }

    public void testElectionWithTwoMembers() throws Exception {
        createNode(0, "A");
        createNode(1, "B");
        View view=createView();
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        assertOneLeader();
        System.out.printf("%s\n", print());
        waitUntilVotingThreadHasStopped();
    }

    /** Tests adding a third member. After {A,B} has formed, this should not change anything */
    public void testThirdMember() throws Exception {
        createNode(0, "A");
        createNode(1, "B");
        View view=createView();
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());

        System.out.println("Adding 3rd member:");
        createNode(2, "C");
        view=createView();
        cluster.handleView(view);
        assertOneLeader();
        System.out.printf("%s\n", print());
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** Tests A -> ABC */
    public void testGoingFromOneToThree() throws Exception {
        createNode(0, "A");
        View view=createView();
        cluster.handleView(view);
        Util.waitUntilTrue(2000, 200, () -> rafts[0].isLeader());
        assert !rafts[0].isLeader();

        createNode(1, "B");
        createNode(2, "C");
        view=createView();
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** {} -> {ABC} */
    public void testGoingFromZeroToThree() throws Exception {
        createNode(0, "A");
        createNode(1, "B");
        createNode(2, "C");
        View view=createView();
        // cluster.async(true);
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** Follower is leaving */
    public void testGoingFromThreeToTwo() throws Exception {
        testGoingFromZeroToThree();
        int follower=findFirst(false);
        kill(follower);
        View view=createView();
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    public void testGoingFromThreeToOne() throws Exception {
        testGoingFromZeroToThree();
        int follower=findFirst(false);
        kill(follower);
        follower=findFirst(false);
        kill(follower);

        View view=createView();
        cluster.handleView(view);
        Util.waitUntilTrue(2000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).noneMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** ABC (A=leader) -> BC */
    public void testLeaderLeaving() throws Exception {
        testGoingFromZeroToThree();
        int leader=findFirst(true);
        kill(leader);
        View view=createView();
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** Tests a vote where a non-coord has a longer log */
    public void testVotingFollowerHasLongerLog() throws Exception {
        createNode(0, "A");
        createNode(1, "B");
        createNode(2, "C");

        byte[] data=new byte[4];
        int[] terms={0,1,1,1,2,4,4,5,6,6};
        RaftImpl impl=rafts[2].impl();
        for(int i=1; i < terms.length; i++) {
            LogEntries entries=new LogEntries().add(new LogEntry(terms[i], data));
            impl.handleAppendEntriesRequest(entries, a, i - 1, terms[i - 1], terms[i], 0);
        }

        View view=createView();
        // cluster.async(true);
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());
        assertOneLeader();
        assert !rafts[0].isLeader();
        assert rafts[2].isLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
        int new_term=terms[terms.length-1]+1;
        assertTerm(new_term, () -> String.format("expected term=%d, actual:\n%s", new_term, print()));
    }

    /** {A}, {B}, {C} -> {A,B,C} */
    public void testMerge() throws Exception {
        createNode(0, "A");
        createNode(1, "B");
        createNode(2, "C");
        View v1,v2,v3;
        nodes[0].handleView(v1=View.create(a, 1, a));
        nodes[1].handleView(v2=View.create(b, 1, b));
        nodes[2].handleView(v3=View.create(c, 1, c));

        View mv=new MergeView(b, 2, Arrays.asList(b,a,c), Arrays.asList(v2,v1,v3));
        for(RaftNode n: nodes)
            n.handleView(mv);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }


    protected void waitUntilVotingThreadHasStopped() throws TimeoutException {
        Util.waitUntil(5000, 100, () -> Stream.of(elections)
                         .allMatch(el -> el == null || !el.isVotingThreadRunning()),
                       this::printVotingThreads);
    }

    // Checks that there is 1 leader in the cluster and the rest are followers
    protected void assertOneLeader() {
        assert Stream.of(rafts).filter(r -> r != null && r.isLeader()).count() == 1 : print();
    }

    protected void assertSameTerm(Supplier<String> message) {
        int term=-1;
        for(int i=0; i < elections.length; i++) {
            if(elections[i] == null)
                continue;
            if(term == -1)
                term=elections[i].raft().currentTerm();
            else
                assert term == elections[i].raft().currentTerm() : message.get();
        }
    }

    protected void assertTerm(int expected_term, Supplier<String> message) {
        for(int i=0; i < elections.length; i++) {
            if(elections[i] == null)
                continue;
            assert expected_term == elections[i].raft().currentTerm() : message.get();
        }
    }


    protected void kill(int index) {
        System.out.printf("-- killing node %d (%s)\n", index, elections[index].getAddress());
        cluster.remove(nodes[index].getAddress());
        nodes[index].stop();
        nodes[index].destroy();
        nodes[index]=null;
        elections[index]=null;
        rafts[index]=null;
    }

    protected int findFirst(boolean leader) {
        for(int i=rafts.length-1; i >= 0; i--) {
            if(rafts[i] != null && rafts[i].isLeader() == leader)
                return i;
        }
        return -1;
    }

    protected String print() {
        return Stream.of(elections).filter(Objects::nonNull)
          .map(el -> String.format("%s: leader=%s, term=%d",
                                   el.getAddress(), el.raft().leader(), el.raft().currentTerm()))
          .collect(Collectors.joining("\n"));
    }


    protected String printVotingThreads() {
        return Stream.of(elections).filter(Objects::nonNull)
          .map(e -> String.format("%s: voting thread=%b", e.getAddress(), e.isVotingThreadRunning()))
          .collect(Collectors.joining("\n"));
    }

    protected RaftNode createNode(int index, String name) throws Exception {
        rafts[index]=new RAFT().raftId(name).members(mbrs).logPrefix("sync-electiontest-" + name)
          .resendInterval(600_000) // long to disable resending by default
          .stateMachine(new DummyStateMachine())
          .synchronous(true).setAddress(addrs[index]);
        elections[index]=new ELECTION().raft(rafts[index]).setAddress(addrs[index]);
        RaftNode node=nodes[index]=new RaftNode(cluster, new Protocol[]{elections[index], rafts[index]});
        node.init();
        cluster.add(addrs[index], node);
        node.start();
        return node;
    }

    protected View createView() {
        List<Address> l=Stream.of(nodes).filter(Objects::nonNull).map(RaftNode::getAddress).collect(Collectors.toList());
        return l.isEmpty()? null : View.create(l.get(0), view_id++, l);
    }

    protected static Address createAddress(String name) {
        ExtendedUUID.setPrintFunction(RAFT.print_function);
        return ExtendedUUID.randomUUID(name).put(RAFT.raft_id_key, Util.stringToBytes(name));
    }
}
