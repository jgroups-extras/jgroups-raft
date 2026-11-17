package org.jgroups.protocols.raft.election;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

import org.jgroups.Global;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.BlockingMessageInterceptor;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * A leader removed from the members while the election round that elected it is still running.
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class LeaderSelfRemovalDuringElectionTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

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

    public void testLeaderSteppingDownBeforeRoundEndsTriggersNewElection(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster();

        RAFT removed = raft(0);
        short electionId = election(0).getId();

        // Blocks the voting thread while it announces the first elected leader.
        AtomicBoolean firstAnnouncement = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m ->
                m.getHeader(electionId) instanceof LeaderElected && firstAnnouncement.getAndSet(false));

        // A coordinates the first election and, with every log empty, elects itself.
        cluster.handleView(createView(0, 0, 1, 2));
        assertThat(eventually(() -> interceptor.numberOfBlockedMessages() == 1, 10, TimeUnit.SECONDS)).isTrue();
        assertThat(removed.isLeader()).isTrue();

        // A commits its own removal while the round that elected it has not finished.
        try {
            removed.removeServer(removed.raftId()).get(10, TimeUnit.SECONDS);
            assertThat(eventually(() -> removed.role().equals("Learner"), 10, TimeUnit.SECONDS)).isTrue();
            assertThat(removed.leader()).isNull();
        } finally {
            interceptor.releaseNext();
        }

        waitUntilLeaderElected(10_000, 0, 1, 2);
        assertThat(removed.leader()).isNotEqualTo(address(0));
    }
}
