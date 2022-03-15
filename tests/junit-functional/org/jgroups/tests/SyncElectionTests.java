package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
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
    protected final List<String> mbrs=Arrays.asList("A", "B", "C");
    protected final RaftCluster  cluster=new RaftCluster();
    protected RAFT[]             rafts=new RAFT[3];
    protected ELECTION[]         elections=new ELECTION[3];
    protected RaftNode[]         nodes=new RaftNode[3];


    @AfterMethod
    protected void destroy() throws Exception {
        for(int i=nodes.length-1; i >= 0; i--) {
            if(nodes[i] != null) {
                nodes[i].stop();
                nodes[i].destroy();
            }
            if(rafts[i] != null)
                rafts[i].deleteLog().deleteSnapshot();
        }
        cluster.clear();
    }

    /** All members have the same (initial) logs, so any member can be elected as leader */
    public void testSimpleElection() throws Exception {
        createNode(0, "A");
        View view=createView();
        cluster.handleView(view);
        assert !rafts[0].isLeader();
        elections[0].noElections(false);
        elections[0].startElectionTimer();
        Util.waitUntilTrue(5000, 500, () -> rafts[0].isLeader());
        assert !rafts[0].isLeader();
    }

    public void testElectionwithTwoMembers() throws Exception {
        createNode(0, "A");
        createNode(1, "B");
        View view=createView();
        Stream.of(elections[0], elections[1]).forEach(el -> el.noElections(false));
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        assertOneLeader();
        System.out.printf("%s\n", print());
        waitUntilHeartbeatAndElectionTimersHaveStopped();
    }

    /** Tests adding a third member. After {A,B} has formed, this should not change anything */
    public void testThirdMember() throws Exception {
        createNode(0, "A");
        createNode(1, "B");
        View view=createView();
        Stream.of(elections[0], elections[1]).forEach(el -> el.noElections(false));
        cluster.handleView(view);
        Util.waitUntilTrue(5000, 100, () -> Arrays.stream(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("%s\n", print());

        System.out.println("Adding 3rd member:");
        createNode(2, "C");
        view=createView();
        elections[2].noElections(false);
        cluster.handleView(view);
        assertOneLeader();
        System.out.printf("%s\n", print());
        waitUntilHeartbeatAndElectionTimersHaveStopped();
        assertSameTerm(this::print);
    }

    protected void waitUntilHeartbeatAndElectionTimersHaveStopped() throws TimeoutException {
        Util.waitUntil(50000, 100, () -> Stream.of(elections)
                         .allMatch(el -> el == null || (!el.isElectionTimerRunning() && !el.isHeartbeatTaskRunning())),
                       this::printTimers);
    }

    // Checks that there is 1 leader in the cluster
    protected void assertOneLeader() {
        assert Stream.of(rafts).filter(r -> r != null && r.isLeader()).count() == 1 : print();
        assert Stream.of(elections).filter(e -> e != null && e.role() == Role.Leader).count() == 1 : print();
    }

    protected void assertSameTerm(Supplier<String> message) {
        int term=-1;
        for(int i=0; i < elections.length; i++) {
            if(elections[i] == null)
                continue;
            if(term == -1)
                term=elections[i].raft().currentTerm();
            else assert term == elections[i].raft().currentTerm() : message.get();
        }
    }


    protected ELECTION leader() {
        for(ELECTION el: elections)
            if(el != null && el.role() == Role.Leader)
                return el;
        return null;
    }

    protected String print() {
        return Stream.of(elections).filter(Objects::nonNull)
          .map(el -> String.format("%s: leader=%s, role=%s, term=%d",
                                   el.getAddress(), el.raft().leader(), el.role(), el.raft().currentTerm()))
          .collect(Collectors.joining("\n"));
    }

    protected String printTimers() {
        return Stream.of(elections).filter(Objects::nonNull)
          .map(e -> String.format("%s: heartbeats=%b, elections=%b", e.getAddress(), e.isHeartbeatTaskRunning(),
                                  e.isElectionTimerRunning()))
          .collect(Collectors.joining("\n"));
    }

    protected RaftNode createNode(int index, String name) throws Exception {
        rafts[index]=new RAFT().raftId(name).members(mbrs).logPrefix("sync-electiontest-" + name)
          .resendInterval(600_000) // long to disable resending by default
          .stateMachine(new DummyStateMachine())
          .synchronous(true).setAddress(addrs[index]);
        elections[index]=new ELECTION().noElections(true).raft(rafts[index]).useViews(true)
          .timer(new TimeScheduler3()).setAddress(addrs[index]);
        RaftNode node=nodes[index]=new RaftNode(cluster, new Protocol[]{elections[index], rafts[index]});
        node.init();
        cluster.add(addrs[index], node);
        node.start();
        return node;
    }

    protected View createView() {
        List<Address> l=Stream.of(nodes).filter(Objects::nonNull).map(RaftNode::getAddress).collect(Collectors.toList());
        return l.isEmpty()? null : View.create(l.get(0), 1, l);
    }

    protected static Address createAddress(String name) {
        ExtendedUUID.setPrintFunction(RAFT.print_function);
        return ExtendedUUID.randomUUID(name).put(RAFT.raft_id_key, Util.stringToBytes(name));
    }
}
