package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.util.Bits;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.jgroups.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

/**
 * Tests replaying of requests after leader changes, to advance commit index<be/>
 * ref: https://github.com/belaban/jgroups-raft/issues/122
 * @author Bela Ban
 * @since  1.0.7
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class SyncLeaderCrashTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {
    protected int                   view_id=1;
    protected final byte[]          DATA=new byte[Integer.BYTES];

    {
        clusterSize = 3;
        createManually = true;
    }


    @BeforeMethod
    protected void init() {
        view_id=1;
        Bits.writeInt(1, DATA, 0);
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }

    public void testsLeaderCrash(Class<?> ignore) throws Exception {
        prepare();

        System.out.println("-- Adding requests 5, 6 and 7 to A, B and C (not yet committing them); then crashing A");
        for(RAFT r: rafts()) {
            for(int i=5; i <= 7; i++) {
                Log l=r.log();
                long prev_term=l.get(i-1).term();
                LogEntries entries=new LogEntries().add(new LogEntry(9, DATA));
                AppendResult ar = r.impl().handleAppendEntriesRequest(entries, address(0),i-1, prev_term, 9, 4);
                assert ar != null && ar.success() : String.format("%s failed on %d with %s", r.raftId(), i, ar);
            }
        }
        close(0);
        View v=createView(view_id++, 1, 2);
        cluster.handleView(v);
        waitUntilLeaderElected(5_000, 1, 2);
        System.out.printf("\n-- Terms after leader A left:\n\n%s\n-- Indices:\n%s\n\n", printTerms(), printIndices(null));
        assertIndices(7, 4);

        RAFT leader=Stream.of(rafts()).filter(r -> r != null && r.isLeader()).findFirst().orElse(null);
        System.out.printf("-- new leader: %s%n", leader);
        assert leader != null;
        System.out.printf("-- Leader: %s, commit-table:\n%s\n", leader.getAddress(), leader.commitTable());

        leader.flushCommitTable();
        leader.flushCommitTable();
        System.out.printf("-- Indices:\n%s\n\n", printIndices(null));
        assertIndices(7, 7);

        System.out.printf("-- State machines:\n%s\n", printStateMachines());
        assert Arrays.stream(rafts())
                .map(RAFT::stateMachine)
                .map(sm -> (CounterStateMachine) sm)
                .filter(Objects::nonNull)
                .allMatch(sm -> sm.counter() == 7)
          : String.format("expected counter of 7, actual:\n%s\n", printStateMachines());

        assert leader.requestTableSize() == 0 : String.format("req_table should be 0, but is %d", leader.requestTableSize());

        // restart A and see if indixes and state machines match
        System.out.println("\n-- Restarting A");
        createCluster();
        v=createView(view_id++, 1, 2, 0);
        cluster.handleView(v);

        leader.flushCommitTable();
        leader.flushCommitTable();
        leader.flushCommitTable();

        System.out.printf("\n-- Indices:\n%s\n\n", printIndices(null));
        assertIndices(7, 7);

        System.out.printf("-- State machines:\n%s\n", printStateMachines());
        assert Arrays.stream(rafts())
                .map(RAFT::stateMachine)
                .map(sm -> (CounterStateMachine) sm)
                .filter(Objects::nonNull)
                .allMatch(sm -> sm.counter() == 7)
          : String.format("expected counter of 7, actual:\n%s\n", printStateMachines());
    }


    /** Creates A,B,C, sends 4 requests and sets commit-index=last-appended to 4 */
    void prepare() throws Exception {
        withClusterSize(3);
        createCluster();
        makeLeader(0); // A is leader
        cluster.handleView(createView(view_id++, 0, 1, 2));
        waitUntilVotingThreadStops(5_000, 0, 1, 2);
        waitUntilLeaderElected(5_000, 0, 1, 2);

        long[] terms={2,5,5,7};

        RAFT r=raft(0);
        assert r.isLeader() : dumpLeaderAndTerms();
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

        raft(0).flushCommitTable(); // updates the commit index
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
            RAFT r=raft(index++);
            if(r == null && expected_terms == null)
                continue;
            long[] actual_terms=terms(r);
            assert Arrays.equals(expected_terms, actual_terms) :
              String.format("%s: expected terms: %s, actual terms: %s", r.getAddress(),
                            Arrays.toString(expected_terms), Arrays.toString(actual_terms));
        }
    }

    protected void assertIndices(int expected_last_appended, int expected_commit) {
        for(RAFT r: rafts()) {
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
            return Stream.of(rafts()).filter(Objects::nonNull).map(this::printIndices).collect(Collectors.joining("\n"));
        return String.format("%s: commit=%d, log-commit=%d, last=%d, log-last=%d", r.getAddress(),
                             r.commitIndex(), r.log().commitIndex(), r.lastAppended(), r.log().lastAppended());
    }

    /** Make the node at index leader, and everyone else follower (ignores election) */
    protected void makeLeader(int index) {
        Address leader=raft(index).getAddress();
        long term = raft(index).currentTerm();
        for(int i=0; i < rafts().length; i++) {
            if(raft(i) == null)
                continue;
            raft(i).setLeaderAndTerm(leader, term + 1);
        }
    }

    protected String printTerms() {
        StringBuilder sb=new StringBuilder("    1 2 3 4 5 6 7\n    -------------\n");
        List<String> mbrs = new ArrayList<>(getRaftMembers());
        for(int i=0; i < mbrs.size(); i++) {
            String name=mbrs.get(i);
            RAFT r=raft(i);
            if(r == null) {
                sb.append(name).append(":  XX\n");
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
        return Stream.of(rafts()).filter(Objects::nonNull).map(r -> String.format("%s: %s", r.getAddress(), r.stateMachine()))
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

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.synchronous(true).stateMachine(new CounterStateMachine());
    }
}
