package org.jgroups.raft.tests.election;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.raft.tests.harness.CheckPoint;
import org.jgroups.raft.tests.harness.RaftAssertion;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = "determine-data-provider")
public class DetermineLeaderBreakdownTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    private static final byte[] BUF = {};

    private CheckPoint checkPoint;

    {
        createManually = true;
        checkPoint = new CheckPoint();
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
        checkPoint = new CheckPoint();
    }

    public void testQuorumLostWhileDeterminingLeader(Class<?> ignore) throws Exception {
        long viewId = 0;
        withClusterSize(3);
        createCluster();

        triggerForever("%s_DETERMINE_IN_CONTINUE", 1, 2);
        triggerForever("%s_DETERMINE_OUT_CONTINUE", 1, 2);

        // Install view with all members needed for election to succeed.
        View v1 = createView(viewId++, 0, 1, 2);
        cluster.handleView(v1);

        // Wait until trying to determine leader.
        checkPoint.awaitStrict("A_DETERMINE_IN", 10, TimeUnit.SECONDS);
        checkPoint.trigger("A_DETERMINE_IN_CONTINUE");

        // Leader `A` is selected based on the complete view with all the votes.
        checkPoint.awaitStrict("A_DETERMINE_OUT", 10, TimeUnit.SECONDS);

        // Install new view without quorum needed for safety.
        View v2 = createView(viewId++, 0);
        cluster.handleView(v2);

        assertThat(raft(0).leader()).isNull();

        // Let the method return and install A as leader.
        checkPoint.trigger("A_DETERMINE_OUT_CONTINUE");

        System.out.println("-- waiting leader to remain null without majority");

        // Utilize the eventually method to make sure the leader is always null in the next 3 seconds.
        assertThat(eventually(() -> !election(0).isVotingThreadRunning(), 5, TimeUnit.SECONDS)).isTrue();
        BooleanSupplier bs = () -> raft(0).leader() != null;
        assertThat(eventually(bs, 3, TimeUnit.SECONDS))
                .withFailMessage(this::dumpLeaderAndTerms)
                .isFalse();

        // Assert not possible to send operation.
        RaftAssertion.assertLeaderlessOperationThrows(() -> raft(0).set(BUF, 0, 0));
    }

    public void testDeterminedLeaderLeave(Class<?> ignore) throws Exception {
        long viewId = 0;
        withClusterSize(3);
        createCluster();

        triggerForever("%s_DETERMINE_IN_CONTINUE", 1, 2);
        triggerForever("%s_DETERMINE_OUT_CONTINUE", 1, 2);

        // Node `C` has the longest log.
        setLog(raft(2), 1, 1, 1, 1);

        // Install new view to trigger election.
        View v1 = createView(viewId++, 0, 1, 2);
        cluster.handleView(v1);

        // Wait until trying to determine leader.
        checkPoint.awaitStrict("A_DETERMINE_IN", 10, TimeUnit.SECONDS);

        // Determines the leader based on view {A, B, C}.
        checkPoint.triggerForever("A_DETERMINE_IN_CONTINUE");
        checkPoint.awaitStrict("A_DETERMINE_OUT", 10, TimeUnit.SECONDS);

        // Now all the votes are collected for view {A, B, C}, where `C` should be elected.
        // However, we change the view removing C before the leader is set locally.
        View v2 = createView(viewId++, 0, 1);
        cluster.handleView(v2);

        // Check leader is still null before the return.
        assertThat(raft(0).leader()).isNull();
        long term = raft(0).currentTerm();

        // Let the method return and try to install C as leader.
        checkPoint.triggerForever("A_DETERMINE_OUT_CONTINUE");

        // The coordinator identifies the view change and the leader is lost.
        // The voting thread should run again to try elect in the new view.
        System.out.println("-- waiting new leader elected");
        waitUntilLeaderElected(10_000, 0, 1);

        // Assert the new leader is either A or B.
        assertThat(leaders())
                .withFailMessage(this::dumpLeaderAndTerms)
                .hasSize(1)
                .containsAnyElementsOf(List.of(address(0), address(1)));

        // Since another voting round is triggered, the term is increased.
        assertThat(term).isLessThan(raft(0).currentTerm());
    }

    private void triggerForever(String template, int ... indexes) {
        for (int index : indexes) {
            RAFT r = raft(index);
            if (r == null) continue;

            checkPoint.triggerForever(String.format(template, r.raftId()));
        }
    }

    private void setLog(RAFT raft, int... terms) {
        Log log = raft.log();
        long index = log.lastAppended();
        LogEntries le = new LogEntries();
        for (int term : terms)
            le.add(new LogEntry(term, BUF));
        log.append(index + 1, le);
    }

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }

    @Override
    protected BaseElection createNewElection() throws Exception {
        BaseElection be = super.createNewElection();
        ((DetermineLeaderCheckpoint) be).checkPoint = checkPoint;
        return be;
    }

    @DataProvider(name = "determine-data-provider")
    protected Object[][] dataProvider() {
        return new Object[][] {
                { DetermineLeaderCheckpoint.class }
        };
    }

    public static class DetermineLeaderCheckpoint extends ELECTION {
        private CheckPoint checkPoint;

        public DetermineLeaderCheckpoint() {
            setId(ELECTION_ID);
            level("trace");
        }

        @Override
        protected Address determineLeader() {
            checkPoint.trigger(String.format("%s_DETERMINE_IN", raft.raftId()));
            checkPoint.uncheckedAwaitStrict(String.format("%s_DETERMINE_IN_CONTINUE", raft.raftId()), 10, TimeUnit.SECONDS);

            Address a = super.determineLeader();

            checkPoint.trigger(String.format("%s_DETERMINE_OUT", raft.raftId()));
            while (true) {
                try {
                    checkPoint.awaitStrict(String.format("%s_DETERMINE_OUT_CONTINUE", raft.raftId()), 10, TimeUnit.SECONDS);
                    break;
                } catch (InterruptedException e) {
                    Thread.interrupted();
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }

            return a;
        }
    }
}
