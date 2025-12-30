package org.jgroups.protocols.raft.election;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.DummyStateMachine;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.raft.tests.harness.CheckPoint;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class LeaderLeavingTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    private final static byte[] BUF = {};
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

    public void testLeaderLeftDuringElection(Class<?> ignore) throws Exception {
        long viewId = 0;
        withClusterSize(3);
        createCluster();

        // Set a long log for node B so it is elected leader.
        setLog(raft(1), 1, 1, 1, 1);

        // Trigger forever so we don't block on other nodes.
        checkPoint.triggerForever("C_ENTER_SET_LAT_CONTINUE");
        checkPoint.triggerForever("B_ENTER_SET_LAT_CONTINUE");

        // Asynchronously install the view. Running in the same thread can deadlock.
        View v1 = createView(viewId++, 0, 1, 2);
        CompletableFuture<Void> f1 = async(() -> cluster.handleView(v1));

        // Await until node A (the coordinator) tries to install the new elected leader.
        // We block the operation and change the view removing the elected leader.
        checkPoint.awaitStrict("A_ENTER_SET_LAT", 10, SECONDS);
        LOGGER.info("-- installing second view without leader");
        View v2 = createView(viewId++, 0, 2);
        CompletableFuture<Void> f2 = async(() -> cluster.handleView(v2));

        // Let the voting thread continue processing.
        // It will send the elected message for the previous leader B.
        // But since it left the cluster, the election keeps running.
        checkPoint.triggerForever("A_ENTER_SET_LAT_CONTINUE");

        LOGGER.info("-- waiting leader to be elected between remaining nodes");
        // A new leader is elected.
        waitUntilLeaderElected(10_000, 0, 2);

        // Assert the leader is one of the remaining nodes.
        assertThat(leader(0, 1))
                .withFailMessage(this::dumpLeaderAndTerms)
                .satisfiesAnyOf(
                        r -> assertThat(r.raftId()).isEqualTo(raft(0).raftId()),
                        r -> assertThat(r.raftId()).isEqualTo(raft(2).raftId()));

        f1.get(10, SECONDS);
        f2.get(10, SECONDS);
    }

    private void setLog(RAFT raft, int... terms) {
        Log log = raft.log();
        long index = log.lastAppended();
        LogEntries le = new LogEntries();
        for (int term : terms)
            le.add(new LogEntry(term, BUF));
        log.append(index + 1, le);
    }

    private RAFT leader(int ... indexes) {
        return IntStream.of(indexes)
                .mapToObj(this::raft)
                .filter(Objects::nonNull)
                .filter(RAFT::isLeader)
                .findFirst()
                .orElseThrow();
    }

    @Override
    protected RAFT newRaftInstance() {
        return new RAFT() {
            @Override
            public RAFT setLeaderAndTerm(Address new_leader, long new_term) {
                checkPoint.trigger(String.format("%s_ENTER_SET_LAT", raft_id));
                checkPoint.uncheckedAwaitStrict(String.format("%s_ENTER_SET_LAT_CONTINUE", raft_id), 10, SECONDS);
                return super.setLeaderAndTerm(new_leader, new_term);
            }
        };
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
