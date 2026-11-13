package org.jgroups.protocols.raft.election;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Elections in a view where the coordinator is a Learner and the voters are absent.
 *
 * @see <a href="https://github.com/jgroups-extras/jgroups-raft/issues/493">GH-493</a>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class LearnerOnlyViewElectionTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    private static final byte[] BUF = { 0x1 };

    {
        createManually = true;
    }

    @AfterMethod
    protected void destroy() throws Exception {
        destroyCluster();
    }

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        // A is the single voter (majority of 1), and B is a Learner.
        raft.members(List.of("A"));
    }

    public void testLoneLearnerCoordinatorDoesNotStartElection(Class<?> ignore) throws Exception {
        withClusterSize(2);
        createCluster();

        RAFT learner = raft(1);
        assertThat(learner.role()).isEqualTo("Learner");

        // B is alone and therefore the view coordinator, but no voter is present to elect anyone.
        cluster.handleView(createView(0, 1));

        assertThat(election(1).isVotingThreadRunning()).isFalse();
        assertThat(learner.currentTerm()).isZero();
        assertThat(learner.leader()).isNull();
    }

    public void testVotingRoundWithoutVotersDoesNotAdvanceTerm(Class<?> ignore) throws Exception {
        withClusterSize(2);
        createCluster();

        RAFT learner = raft(1);
        cluster.handleView(createView(0, 1));

        // Forcing an election skips the view checks and runs a voting round directly on the coordinator.
        CompletableFuture<Address> cf = election(1).startForcedElection(null).toCompletableFuture();

        assertThatThrownBy(() -> cf.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(election(1).isVotingThreadRunning()).isFalse();
        assertThat(learner.currentTerm()).isZero();
        assertThat(learner.leader()).isNull();
    }

    public void testVoterJoiningLoneLearnerBecomesLeader(Class<?> ignore) throws Exception {
        withClusterSize(2);
        createCluster();

        RAFT voter = raft(0);
        RAFT learner = raft(1);

        // B starts alone, A is unreachable.
        cluster.handleView(createView(0, 1));

        // A joins B's view, and B remains the view coordinator.
        // The previous view dropped A from the mock cluster, so it is added back first.
        cluster.add(address(0), node(0));
        cluster.handleView(createView(1, 1, 0));

        assertThat(eventually(() -> voter.isLeader()
                && address(0).equals(learner.leader())
                && learner.currentTerm() == voter.currentTerm(), 10, TimeUnit.SECONDS))
                .withFailMessage(this::dumpLeaderAndTerms)
                .isTrue();
    }

    public void testLearnerJoiningVoterCatchesUp(Class<?> ignore) throws Exception {
        withClusterSize(2);
        createCluster();

        RAFT voter = raft(0);
        RAFT learner = raft(1);

        // A starts alone and, as the single voter, elects itself.
        cluster.handleView(createView(0, 0));
        assertThat(eventually(voter::isLeader, 10, TimeUnit.SECONDS))
                .withFailMessage(this::dumpLeaderAndTerms)
                .isTrue();

        for (int i = 0; i < 5; i++) {
            voter.set(BUF, 0, BUF.length, 10, TimeUnit.SECONDS);
        }

        // B joins A's view.
        // The previous view dropped B from the mock cluster, so it is added back first.
        cluster.add(address(1), node(1));
        cluster.handleView(createView(1, 0, 1));

        assertThat(eventually(() -> address(0).equals(learner.leader())
                && learner.currentTerm() == voter.currentTerm()
                && learner.lastAppended() == voter.lastAppended()
                && learner.commitIndex() == voter.commitIndex(), 10, TimeUnit.SECONDS))
                .withFailMessage(() -> String.format("leader: %s (term=%d, last=%d, commit=%d), learner: %s (term=%d, last=%d, commit=%d)",
                        voter.leader(), voter.currentTerm(), voter.lastAppended(), voter.commitIndex(),
                        learner.leader(), learner.currentTerm(), learner.lastAppended(), learner.commitIndex()))
                .isTrue();
        assertThat(learner.role()).isEqualTo("Learner");
    }
}
