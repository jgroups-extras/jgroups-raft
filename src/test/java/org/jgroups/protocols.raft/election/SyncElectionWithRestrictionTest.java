package org.jgroups.protocols.raft.election;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.RaftImpl;
import org.jgroups.raft.DummyStateMachine;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

/**
 * Uses the synchronous test framework to test {@link ELECTION}. Tests the election restrictions described in 5.4.1
 * (Fig. 8) in the Raft paper [1]<br/>
 * [1] https://raft.github.io/raft.pdf
 * @author Bela Ban
 * @since  1.0.7
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider=ALL_ELECTION_CLASSES_PROVIDER)
public class SyncElectionWithRestrictionTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    protected int                 view_id=1;
    protected static final byte[] DATA={1,2,3,4,5};

    {
        // Following the paper nomenclature, S1, S2, S3, S4, S5.
        clusterSize = 5;
        createManually = true;
    }

    @BeforeMethod
    protected void init() {
        view_id=1;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
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
    public void testScenarioD(Class<?> ignore) throws Exception {
        createScenarioC();
        LOGGER.info("-- Initial: %n{}", printTerms());
        close(0);
        makeLeader(4);
        View v=createView(view_id++, 4, 1, 2, 3);
        cluster.handleView(v);
        LOGGER.info("-- After killing S1 and making S5 leader:%n{}", printTerms());
        assertTerms(null, new long[]{1,2}, new long[]{1,2}, new long[]{1}, new long[]{1,3});
        RAFT r5 = waitUntilNodeLeader(4);
        r5.flushCommitTable();
        LOGGER.info("-- After S1 resending messages:%n{}", printTerms());
        long[] expected={1,3};
        assertTerms(null, expected, expected, expected, expected);
    }

    /** Tests scenario E in fig.8 5.4.1 [1]: S1 replicates term=4 to S2 and S3, then crashes: either S2 or S3 will be
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
    public void testScenarioE(Class<?> ignore) throws Exception {
        createScenarioC();
        View v=createView(view_id++, 0, 1, 2, 3, 4);
        cluster.handleView(v);

        // append term 4 on S2 and S3:
        RAFT r1=waitUntilNodeLeader(0);
        LOGGER.info("-- flushing tables");
        r1.flushCommitTable(address(1));
        r1.flushCommitTable(address(2));

        LOGGER.info("-- Initial:%n{}", printTerms());
        close(0);
        v=createView(view_id++, 1, 2, 3, 4);
        cluster.handleView(v);
        LOGGER.info("-- After killing S1:%n{}", printTerms());
        assertTerms(null, new long[]{1,2,4}, new long[]{1,2,4}, new long[]{1}, new long[]{1,3});

        // start voting to find current leader:
        BaseElection e2=election(1);
        e2.startVotingThread();
        waitUntilVotingThreadStops(5_000, 1, 2, 3, 4);
        waitUntilLeaderElected(5_000, 1, 2, 3, 4);

        String leaderAndTerms = dumpLeaderAndTerms();
        LOGGER.info("-- After the voting phase (either S2 or S3 will be leader):%n{}", leaderAndTerms);
        assertThat(leaders())
                .as(leaderAndTerms)
                .hasSize(1)
                .containsAnyOf(address(1), address(2));

        RAFT leader_raft=raft(1).isLeader()? raft(1) : raft(2);
        assertThat(leader_raft)
                .as(leaderAndTerms)
                .isNotNull()
                .satisfies(r -> assertThat(r.isLeader()).isTrue());

        // first round adjusts match-index for S4 and deletes term=3 at index 2 on S5
        // second round sends term=2 at index 2
        // third round sends term=4 at index 3
        long[] expected={1,2,4};
        waitUntilTerms(10000,1000, expected, leader_raft::flushCommitTable);
        assertTerms(null, expected, expected, expected, expected);
        LOGGER.info("-- final terms:%n{}", printTerms());
    }


    /** Creates scenario C in fig 8 */
    protected void createScenarioC() throws Exception {
        withClusterSize(5);
        createCluster();
        makeLeader(0); // S1 is leader
        Address leader=raft(0).getAddress();
        append(leader, 0, 1, 2, 4);
        append(leader, 1, 1,2);
        append(leader, 2, 1,2);
        append(leader, 3, 1);
        append(leader, 4, 1,3);
    }

    protected void append(Address leader, int index, int ... terms) {
        RaftImpl impl=raft(index).impl();
        for(int i=0; i < terms.length; i++) {
            int curr_index=i+1, prev_index=curr_index-1;
            int prev_term=prev_index == 0? 0 : terms[prev_index-1];
            int curr_term=terms[curr_index-1];
            raft(index).currentTerm(curr_term);
            LogEntries entries=new LogEntries().add(new LogEntry(curr_term, DATA));
            impl.handleAppendEntriesRequest(entries, leader, i, prev_term, curr_term, 0);
        }
    }

    protected void assertTerms(long[] ... exp_terms) {
        int index=0;
        for(long[] expected_terms: exp_terms) {
            RAFT r=raft(index++);
            if(r == null && expected_terms == null)
                continue;
            long[] actual_terms=terms(r);
            assertThat(actual_terms)
                    .as(() -> String.format("%s: expected terms: %s, actual terms: %s", r.getAddress(),
                            Arrays.toString(expected_terms), Arrays.toString(actual_terms)))
                    .isEqualTo(expected_terms);
        }
    }

    protected void waitUntilTerms(long timeout, long interval, long[] expexted_terms, Runnable action) {
        BooleanSupplier bs = () -> Stream.of(rafts()).filter(Objects::nonNull)
                .allMatch(r -> Arrays.equals(terms(r),expexted_terms));
        assertThat(waitUntilTrue(timeout, interval, bs, action))
                .as(this::dumpLeaderAndTerms)
                .isTrue();
    }

    public static boolean waitUntilTrue(long timeout, long interval, BooleanSupplier condition,
                                        Runnable action) {
        long target_time = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() <= target_time) {
            if(condition.getAsBoolean())
                return true;
            if(action != null)
                action.run();
            Util.sleep(interval);
        }

        return false;
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
        StringBuilder sb=new StringBuilder("     1 2 3\n     -----\n");
        List<String> mbrs = new ArrayList<>(getRaftMembers());
        for(int i=0; i < mbrs.size(); i++) {
            String name=mbrs.get(i);
            RAFT r=raft(i);
            if(r == null) {
                sb.append(name).append(": XX\n");
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

    protected static long[] terms(RAFT r) {
        Log l=r.log();
        List<Long> list=new ArrayList<>((int)l.size());
        for(int i=1; i <= l.lastAppended(); i++) {
            LogEntry e=l.get(i);
            list.add(e.term());
        }
        return list.stream().mapToLong(Long::longValue).toArray();
    }

    private RAFT waitUntilNodeLeader(int node) {
        RAFT raft = raft(node);
        assertThat(raft).as("Node at index " + node + " is null").isNotNull();

        Address leaderAddress = raft.getAddress();
        BooleanSupplier bs = () -> Arrays.stream(rafts())
                .allMatch(r -> raft(node).currentTerm() == r.currentTerm() && leaderAddress.equals(r.leader()));
        assertThat(eventually(bs, 5, TimeUnit.SECONDS))
                .as(this::dumpLeaderAndTerms)
                .isTrue();
        return raft;
    }

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.synchronous(true).stateMachine(new DummyStateMachine());
    }
}
