package org.jgroups.protocols.raft.election;

import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.raft.tests.harness.CheckPoint;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = "voting-thread-checkpoint")
public class VotingThreadBreakdownTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

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

    public void testMajorityLostAndGained(Class<?> ignore) throws Exception {
        long viewId = 0;
        withClusterSize(3);
        createCluster();

        // Disable checkpoint for the other nodes.
        triggerForever("%s_STOP_METHOD_IN_WAIT", 1, 2);
        triggerForever("%s_START_METHOD_IN_WAIT", 1, 2);

        // Install view with majority to trigger the election mechanism.
        View v1 = createView(viewId++, 0, 1, 2);
        System.out.println("-- installing first view with all members");
        CompletableFuture<Void> f1 = async(() -> cluster.handleView(v1));

        // New view starts the voting thread.
        checkPoint.awaitStrict("A_START_METHOD_IN", 100, TimeUnit.SECONDS);

        // Install new view without majority.
        View v2 = createView(viewId++, 0);
        CompletableFuture<Void> f2 = async(() -> cluster.handleView(v2));
        checkPoint.awaitStrict("A_STOP_METHOD_IN", 100, TimeUnit.SECONDS);
        checkPoint.trigger("A_STOP_METHOD_IN_WAIT");

        f2.get(10, TimeUnit.SECONDS);

        // Let the original start voting thread run.
        // Trigger forever so subsequent starts can happen automatically without waiting.
        checkPoint.triggerForever("A_START_METHOD_IN_WAIT");
        f1.get(10, TimeUnit.SECONDS);

        // It will verify the majority is lost and try stop the voting thread.
        // We install the new view while the coordinator is trying to stop the voting thread.
        //checkPoint.awaitStrict("A_STOP_METHOD_IN", 10, TimeUnit.SECONDS);
        View v3 = createView(viewId++, 0, 1, 2);

        System.out.printf("-- installing complete view: %s%n", v3);
        cluster.add(node(1).getAddress(), node(1));
        cluster.add(node(2).getAddress(), node(2));
        CompletableFuture<Void> f3 = async(() -> cluster.handleView(v3));

        // Let the voting thread stop.
        // Trigger forever so the election can proceed without waiting.
        checkPoint.triggerForever("A_STOP_METHOD_IN_WAIT");

        // A leader should still be elected.
        waitUntilLeaderElected(10_000, 0, 1, 2);
        f3.get(10, TimeUnit.SECONDS);
    }

    private void triggerForever(String template, int ... indexes) {
        for (int index : indexes) {
            RAFT r = raft(index);
            if (r == null) continue;

            checkPoint.triggerForever(String.format(template, r.raftId()));
        }
    }

    @Override
    protected RaftCluster createNewMockCluster() {
        return new RaftCluster();
    }

    @Override
    protected BaseElection createNewElection() throws Exception {
        BaseElection be = super.createNewElection();
        ((VotingThreadCheckpoint) be).checkPoint = checkPoint;
        return be;
    }

    @DataProvider(name = "voting-thread-checkpoint")
    protected Object[][] dataProvider() {
        return new Object[][] {
                { VotingThreadCheckpoint.class }
        };
    }

    public static class VotingThreadCheckpoint extends ELECTION {
        private CheckPoint checkPoint;

        public VotingThreadCheckpoint() {
            setId(ELECTION_ID);
        }

        @Override
        public BaseElection stopVotingThread() {
            checkPoint.trigger(String.format("%s_STOP_METHOD_IN", raft.raftId()));
            try {
                checkPoint.awaitStrict(String.format("%s_STOP_METHOD_IN_WAIT", raft.raftId()), 10, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            return super.stopVotingThread();
        }

        @Override
        public BaseElection startVotingThread() {
            checkPoint.trigger(String.format("%s_START_METHOD_IN", raft.raftId()));
            try {
                checkPoint.awaitStrict(String.format("%s_START_METHOD_IN_WAIT", raft.raftId()), 10, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            return super.startVotingThread();
        }
    }
}
