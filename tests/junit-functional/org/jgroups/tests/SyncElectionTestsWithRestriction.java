package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.*;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Uses the synchronous test framework to test {@link ELECTION}. Tests the election restrictions described in 5.4.1
 * (Fig. 8) in the Raft paper [1]<br/>
 * [1]
 * @author Bela Ban
 * @since  1.0.7
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class SyncElectionTestsWithRestriction {
    protected final Address       s1,s2,s3,s4,s5;
    protected final Address[]     addrs={s1=createAddress("S1"), s2=createAddress("S2"),
                                         s3=createAddress("S3"), s4=createAddress("S4"),
                                         s5=createAddress("S5")};
    protected final List<String>  mbrs=List.of("S1", "S2", "S3", "S4", "S5");
    protected final RaftCluster   cluster=new RaftCluster();
    protected RAFT[]              rafts=new RAFT[5];
    protected ELECTION[]          elections=new ELECTION[5];
    protected RaftNode[]          nodes=new RaftNode[5];
    protected int                 view_id=1;
    protected static final byte[] DATA={1,2,3,4,5};


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
                rafts[i].deleteLog().deleteSnapshot();
                rafts[i]=null;
            }
            if(elections[i] != null) {
                elections[i].stopVotingThread();
                elections[i]=null;
            }
        }
        cluster.clear();
    }



    /** Tests scenario D in fig.8 5.4.1 [1]: S5 as leader overwrites old (*uncommitted*) entries from (crashed S1):
     * <pre>
     *       1 2 3    1 2 3
     *   ------------------
     *   S1  1 2 4    XXXXX
     *   S2  1 2      1 3
     *   S3  1 2      1 3
     *   S4  1        1 3
     *   S5  1 3      1 3
     *       (c)      (d)
     * </pre>
     */
    public void testScenarioD() throws Exception {
        createScenarioC();
        System.out.printf("-- Initial:\n%s\n", printTerms());
        kill(0, true);
        makeLeader(4);
        View v=View.create(s5, view_id++, s5,s2,s3,s4);
        cluster.handleView(v);
        System.out.printf("-- After killing S1 and making S5 leader:\n%s\n", printTerms());
        assertTerms(null, new int[]{1,2}, new int[]{1,2}, new int[]{1}, new int[]{1,3});
        RAFT r5=rafts[4];
        r5.flushCommitTable();
        System.out.printf("-- After S1 resending messages:\n%s\n\n", printTerms());
        int[] expected={1,3};
        assertTerms(null, expected, expected, expected, expected);
    }

    /** Tests scenario E in fig.8 5.4.1 [1]: S1 replicates term=4 to S2 and S2, then crashes: either S2 or S3 will be
     * the new leader because they have the highest term (4):
     * <pre>
     *       1 2 3    1 2 3        1 2 3
     *   ------------------
     *   S1  1 2 4    1 2 4        XXXXX
     *   S2  1 2      1 2 4        1 2 4
     *   S3  1 2      1 2 4   ==>  1 2 4
     *   S4  1        1            1 2 4
     *   S5  1 3      1 3          1 2 4
     *       (c)      (e)
     * </pre>
     */
    public void testScenarioE() throws Exception {
        createScenarioC();
        View v=createView();
        cluster.handleView(v);
        // append term 4 on S2 and S3:
        RAFT r1=rafts[0];
        r1.flushCommitTable(s2);
        r1.flushCommitTable(s3);

        System.out.printf("-- Initial:\n%s\n", printTerms());
        kill(0, true);
        v=View.create(s2, view_id++, s2,s3,s4,s5);
        cluster.handleView(v);
        System.out.printf("-- After killing S1:\n%s\n", printTerms());
        assertTerms(null, new int[]{1,2,4}, new int[]{1,2,4}, new int[]{1}, new int[]{1,3});

        // start voting to find current leader:
        ELECTION e2=elections[1];
        e2.startVotingThread();
        Util.waitUntilTrue(5000, 200, () -> Stream.of(rafts).filter(Objects::nonNull).anyMatch(RAFT::isLeader));
        System.out.printf("-- After the voting phase (either S2 or S3 will be leader):\n%s\n", printTerms());

        List<Address> leaders=Stream.of(rafts).filter(Objects::nonNull)
          .filter(RAFT::isLeader).map(Protocol::getAddress).collect(Collectors.toList());
        assert leaders.size() == 1;
        assert leaders.contains(s2) || leaders.contains(s3);
        RAFT leader_raft=rafts[1].isLeader()? rafts[1] : rafts[2];

        // first round adjusts match-index for S4 and deletes term=3 at index 2 on S5
        // second round sends term=2 at index 2
        // third round sends term=4 at index 3
        for(int i=1; i <= 3; i++)
            leader_raft.flushCommitTable();

        int[] expected={1,2,4};
        assertTerms(null, expected, expected, expected, expected);
    }


    /** Creates scenario C in fig 8 */
    protected void createScenarioC() throws Exception {
        for(int i=0; i < mbrs.size(); i++)
            createNode(i, mbrs.get(i));
        makeLeader(0); // S1 is leader
        Address leader=rafts[0].getAddress();
        append(leader, 0, 1, 2, 4);
        append(leader, 1, 1,2);
        append(leader, 2, 1,2);
        append(leader, 3, 1);
        append(leader, 4, 1,3);
    }

    protected void append(Address leader, int index, int ... terms) {
        RaftImpl impl=rafts[index].impl();
        for(int i=0; i < terms.length; i++) {
            int curr_index=i+1, prev_index=curr_index-1;
            int prev_term=prev_index == 0? 0 : terms[prev_index-1];
            int curr_term=terms[curr_index-1];
            rafts[index].currentTerm(curr_term);
            LogEntries entries=new LogEntries().add(new LogEntry(curr_term, DATA));
            impl.handleAppendEntriesRequest(entries, leader, i, prev_term, curr_term, 0);
        }
    }

    protected void assertTerms(int[] ... exp_terms) {
        int index=0;
        for(int[] expected_terms: exp_terms) {
            RAFT r=rafts[index++];
            if(r == null && expected_terms == null)
                continue;
            int[] actual_terms=terms(r);
            assert Arrays.equals(expected_terms, actual_terms) :
              String.format("%s: expected terms: %s, actual terms: %s", r.getAddress(),
                            Arrays.toString(expected_terms), Arrays.toString(actual_terms));
        }
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
        StringBuilder sb=new StringBuilder("     1 2 3\n     -----\n");
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

    protected static int[] terms(RAFT r) {
        Log l=r.log();
        List<Integer> list=new ArrayList<>(l.size());
        for(int i=1; i <= l.lastAppended(); i++) {
            LogEntry e=l.get(i);
            list.add(e.term());
        }
        return list.stream().mapToInt(Integer::intValue).toArray();
    }

    protected void kill(int index, boolean remove_log) throws Exception {
        cluster.remove(nodes[index].getAddress());
        nodes[index].stop();
        nodes[index].destroy();
        nodes[index]=null;
        if(elections[index] != null) {
            elections[index].stopVotingThread();
            elections[index]=null;
        }
        if(rafts[index] != null) {
            if(remove_log)
                rafts[index].deleteLog().deleteSnapshot();
            rafts[index]=null;
        }
    }


    protected RaftNode createNode(int index, String name) throws Exception {
        rafts[index]=new RAFT().raftId(name).members(mbrs).logPrefix("sync-electiontest-restriction-" + name)
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


    protected void waitUntilVotingThreadHasStopped() throws TimeoutException {
        Util.waitUntil(5000, 100, () -> Stream.of(elections)
                         .allMatch(el -> el == null || !el.isVotingThreadRunning()));
    }

}
