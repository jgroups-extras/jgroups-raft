package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.raft.util.Utils;
import org.jgroups.stack.Protocol;
import org.jgroups.tests.election.BaseElectionTest;
import org.jgroups.util.Bits;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jgroups.tests.election.BaseElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

/**
 * Tests replaying of requests after leader changes, to advance commit index<be/>
 * ref: https://github.com/belaban/jgroups-raft/issues/122
 * @author Bela Ban
 * @since  1.0.7
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class SyncLeaderCrashTest extends BaseElectionTest {
    protected final Address         a, b, c;
    protected final Address[]       addrs={a=createAddress("A"), b=createAddress("B"),
                                           c=createAddress("C")};
    protected final List<String>    mbrs=List.of("A", "B", "C");
    protected final RaftCluster     cluster=new RaftCluster();
    protected RAFT[]                rafts=new RAFT[3];
    protected BaseElection[]        elections=new BaseElection[3];
    protected RaftNode[]            nodes=new RaftNode[3];
    protected CounterStateMachine[] sms;
    protected int                   view_id=1;
    protected final byte[]          DATA=new byte[Integer.BYTES];




    @BeforeMethod protected void init() {
        view_id=1;
        sms=new CounterStateMachine[]{new CounterStateMachine(), new CounterStateMachine(), new CounterStateMachine()};
        Bits.writeInt(1, DATA, 0);
    }

    @AfterMethod
    protected void destroy() throws Exception {
        for(int i=nodes.length-1; i >= 0; i--) {
            if(nodes[i] != null) {
                nodes[i].stop();
                nodes[i].destroy();
                nodes[i]=null;
            }
            if(elections[i] != null) {
                elections[i].stopVotingThread();
                elections[i]=null;
            }
            if(rafts[i] != null) {
                Utils.deleteLog(rafts[i]);
                rafts[i]=null;
            }
        }
        cluster.clear();
    }

    public void testsLeaderCrash(Class<?> ignore) throws Exception {
        prepare();

        System.out.println("-- Adding requests 5, 6 and 7 to A, B and C (not yet committing them); then crashing A");
        for(RAFT r: rafts) {
            for(int i=5; i <= 7; i++) {
                Log l=r.log();
                long prev_term=l.get(i-1).term();
                LogEntries entries=new LogEntries().add(new LogEntry(9, DATA));
                AppendResult ar = r.impl().handleAppendEntriesRequest(entries, a,i-1, prev_term, 9, 4);
                assert ar != null && ar.success() : String.format("%s failed on %d with %s", r.raftId(), i, ar);
            }
        }
        kill(0);
        View v=View.create(b, view_id++, b,c);
        cluster.handleView(v);
        waitUntilLeaderElected();
        System.out.printf("\n-- Terms after leader A left:\n\n%s\n-- Indices:\n%s\n\n", printTerms(), printIndices(null));
        assertIndices(7, 4);

        RAFT leader=Stream.of(rafts).filter(r -> r != null && r.isLeader()).findFirst().orElse(null);
        assert leader != null;
        System.out.printf("-- Leader: %s, commit-table:\n%s\n", leader.getAddress(), leader.commitTable());

        leader.flushCommitTable();
        leader.flushCommitTable();
        System.out.printf("-- Indices:\n%s\n\n", printIndices(null));
        assertIndices(7, 7);

        System.out.printf("-- State machines:\n%s\n", printStateMachines());
        assert Stream.of(sms).filter(Objects::nonNull).allMatch(sm -> sm.counter() == 7)
          : String.format("expected counter of 7, actual:\n%s\n", printStateMachines());

        assert leader.requestTableSize() == 0 : String.format("req_table should be 0, but is %d", leader.requestTableSize());

        // restart A and see if indixes and state machines match
        System.out.println("\n-- Restarting A");
        sms[0]=new CounterStateMachine();
        createNode(0, mbrs.get(0));
        v=View.create(leader.getAddress(), view_id++, b,c,a);
        cluster.handleView(v);

        leader.flushCommitTable();
        leader.flushCommitTable();
        leader.flushCommitTable();

        System.out.printf("\n-- Indices:\n%s\n\n", printIndices(null));
        assertIndices(7, 7);

        System.out.printf("-- State machines:\n%s\n", printStateMachines());
        assert Stream.of(sms).filter(Objects::nonNull).allMatch(sm -> sm.counter() == 7)
          : String.format("expected counter of 7, actual:\n%s\n", printStateMachines());
    }


    /** Creates A,B,C, sends 4 requests and sets commit-index=last-appended to 4 */
    void prepare() throws Exception {
        for(int i=0; i < mbrs.size(); i++)
            createNode(i, mbrs.get(i));
        makeLeader(0); // A is leader
        View v=View.create(a, view_id++, a,b,c);
        cluster.handleView(v);
        waitUntilLeaderElected();

        long[] terms={2,5,5,7};

        RAFT r=rafts[0];
        assert r.isLeader();
        Address leader = r.leader();
        r.setLeaderAndTerm(leader, 2);
        r.set(DATA, 0, DATA.length, 5, TimeUnit.SECONDS);
        r.setLeaderAndTerm(leader, 5);
        r.set(DATA, 0, DATA.length, 5, TimeUnit.SECONDS);
        r.set(DATA, 0, DATA.length, 5, TimeUnit.SECONDS);
        r.setLeaderAndTerm(leader, 7);
        r.set(DATA, 0, DATA.length, 5, TimeUnit.SECONDS);

        System.out.printf("terms:\n%s\n", printTerms());
        assertTerms(terms, terms, terms);

        rafts[0].flushCommitTable(); // updates the commit index
        System.out.printf("-- Indices:\n%s\n", printIndices(null));
        assertIndices(4, 4);
    }


    protected static byte[] intToByte(int num) {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(num, val, 0);
        return val;
    }

    protected void assertTerms(long[] ... exp_terms) {
        int index=0;
        for(long[] expected_terms: exp_terms) {
            RAFT r=rafts[index++];
            if(r == null && expected_terms == null)
                continue;
            long[] actual_terms=terms(r);
            assert Arrays.equals(expected_terms, actual_terms) :
              String.format("%s: expected terms: %s, actual terms: %s", r.getAddress(),
                            Arrays.toString(expected_terms), Arrays.toString(actual_terms));
        }
    }

    protected void assertIndices(int expected_last_appended, int expected_commit) {
        for(RAFT r: rafts) {
            if(r == null)
                continue;
            Log l=r.log();
            assert r.lastAppended() == expected_last_appended
              && l.lastAppended() == expected_last_appended
              && r.commitIndex() == expected_commit && l.commitIndex() == expected_commit : printIndices(r);
        }
    }

    protected String printIndices(RAFT r) {
        if(r == null)
            return Stream.of(rafts).filter(Objects::nonNull).map(this::printIndices).collect(Collectors.joining("\n"));
        return String.format("%s: commit=%d, log-commit=%d, last=%d, log-last=%d", r.getAddress(),
                             r.commitIndex(), r.log().commitIndex(), r.lastAppended(), r.log().lastAppended());
    }

    /** Make the node at index leader, and everyone else follower (ignores election) */
    protected void makeLeader(int index) {
        Address leader=rafts[index].getAddress();
        for(int i=0; i < rafts.length; i++) {
            if(rafts[i] == null)
                continue;
            rafts[i].setLeaderAndTerm(leader);
        }
    }

    protected String printTerms() {
        StringBuilder sb=new StringBuilder("    1 2 3 4 5 6 7\n    -------------\n");
        for(int i=0; i < mbrs.size(); i++) {
            String name=mbrs.get(i);
            RAFT r=rafts[i];
            if(r == null) {
                sb.append("XX\n");
                continue;
            }
            Log l=r.log();
            sb.append(name).append(":  ");
            for(int j=1; j <= l.lastAppended(); j++) {
                LogEntry e=l.get(j);
                if(e != null)
                    sb.append(e.term()).append(" ");
            }
            if(r.isLeader())
                sb.append(" (leader)");
            sb.append("\n");
        }
        return sb.toString();
    }

    protected String printStateMachines() {
        return Stream.of(rafts).filter(Objects::nonNull).map(r -> String.format("%s: %s", r.getAddress(), r.stateMachine()))
          .collect(Collectors.joining("\n"));
    }

    protected static long[] terms(RAFT r) {
        Log l=r.log();
        List<Long> list=new ArrayList<>((int)l.size());
        for(int i=1; i <= l.lastAppended(); i++) {
            LogEntry e=l.get(i);
            list.add(e.term());
        }
        return list.stream().mapToLong(Long::longValue).toArray();
    }

    protected void kill(int index) throws Exception {
        cluster.remove(nodes[index].getAddress());
        nodes[index].stop();
        nodes[index].destroy();
        nodes[index]=null;
        if(elections[index] != null) {
            elections[index].stopVotingThread();
            elections[index]=null;
        }
        if(rafts[index] != null) {
            Utils.deleteLog(rafts[index]);
            rafts[index]=null;
        }
        sms[index]=null;
    }


    protected RaftNode createNode(int index, String name) throws Exception {
        rafts[index]=new RAFT().raftId(name).members(mbrs).logPrefix("sync-leadercrash-" + name)
          .resendInterval(600_000) // long to disable resending by default
          .stateMachine(sms[index])
          .synchronous(true).setAddress(addrs[index]);
        elections[index]=instantiate().raft(rafts[index]).setAddress(addrs[index]);
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


    protected void waitUntilLeaderElected() throws TimeoutException {
        // ELECTION2 the voting thread has a delayed start, so we wait for a leader elected instead.
        BooleanSupplier supplier = electionClass == ELECTION2.class
                ? () -> Stream.of(elections).anyMatch(el -> el != null && el.raft().isLeader())
                : () -> Stream.of(elections).allMatch(el -> el == null || !el.isVotingThreadRunning());
        Util.waitUntil(5000, 100, supplier);
    }

}
