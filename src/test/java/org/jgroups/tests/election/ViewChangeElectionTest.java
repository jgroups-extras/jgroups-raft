package org.jgroups.raft.tests.election;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.View;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.LeaderElected;
import org.jgroups.raft.testfwk.BlockingMessageInterceptor;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.raft.tests.DummyStateMachine;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.raft.tests.harness.RaftAssertion;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class ViewChangeElectionTest extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    private static final byte[] BUF = {};


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

    public void testLeaderLostAfterElected(Class<?> ignore) throws Exception {
        long view_id = 0;
        withClusterSize(3);
        createCluster();

        // Define the longest log for C, so it can be the leader.
        setLog(raft(2), 1, 1, 1, 1);

        View view = createView(view_id++, 0, 1, 2);

        // We intercept the first `LeaderElected` message.
        AtomicBoolean onlyOnce = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m -> {
            for (Map.Entry<Short, Header> h : m.getHeaders().entrySet()) {
                if (h.getValue() instanceof LeaderElected && onlyOnce.getAndSet(false)) {
                    // Assert that node C was elected
                    LeaderElected le = (LeaderElected) h.getValue();
                    assertThat(le.leader()).isEqualTo(address(2));
                    return true;
                }
            }
            return false;
        });

        cluster.handleView(view);

        System.out.println("-- wait command intercept");
        assertThat(RaftTestUtils.eventually(() -> interceptor.numberOfBlockedMessages() > 0, 10, TimeUnit.SECONDS)).isTrue();

        // While the message is in-flight, the elected leader leaves the cluster.
        view = createView(view_id++, 0, 1);
        cluster.handleView(view);

        // We can release the elected message.
        interceptor.releaseNext();
        interceptor.assertNoBlockedMessages();

        // Wait until either A or B is elected.
        System.out.println("-- waiting for leader between A and B");
        RAFT[] rafts = new RAFT[] { raft(0), raft(1) };
        BaseRaftElectionTest.waitUntilLeaderElected(rafts, 10_000);

        assertOneLeader();
        assertThat(leaders())
                .withFailMessage(this::dumpLeaderAndTerms)
                .hasSize(1)
                .containsAnyOf(address(0), address(1));
    }

    public void testCoordinatorLeaveBeforeSendingElected(Class<?> ignore) throws Exception {
        long view_id = 0;
        withClusterSize(3);
        createCluster();

        View view = createView(view_id++, 0, 1, 2);

        // We intercept the first `LeaderElected` message.
        AtomicBoolean onlyOnce = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m -> {
            for (Map.Entry<Short, Header> h : m.getHeaders().entrySet()) {
                if (h.getValue() instanceof LeaderElected && onlyOnce.getAndSet(false)) {
                    // Assert that node C was elected
                    LeaderElected le = (LeaderElected) h.getValue();
                    assertThat(le.leader()).isEqualTo(address(0));
                    return true;
                }
            }
            return false;
        });

        cluster.handleView(view);

        System.out.println("-- wait command intercept");
        assertThat(RaftTestUtils.eventually(() -> interceptor.numberOfBlockedMessages() > 0, 10, TimeUnit.SECONDS)).isTrue();

        // While the message is in-flight, the elected leader leaves the cluster.
        view = createView(view_id++, 1, 2);

        System.out.printf("-- install new view %s%n", view);
        cluster.handleView(view);

        // We can release the elected message.
        interceptor.releaseNext();
        interceptor.assertNoBlockedMessages();

        // Wait until either A or B is elected.
        System.out.println("-- waiting for leader between B and C");
        RAFT[] rafts = new RAFT[] { raft(1), raft(2) };
        BaseRaftElectionTest.waitUntilLeaderElected(rafts, 10_000);

        assertThat(clusterLeaders(1, 2)).withFailMessage(this::dumpLeaderAndTerms)
                .hasSize(1)
                .containsAnyOf(address(1), address(2));

        // Operations on node A fail!
        RaftAssertion.assertLeaderlessOperationThrows(() -> raft(0).set(new byte[0], 0, 0, 500, TimeUnit.MILLISECONDS));

        // Operations on other nodes succeed.
        RAFT leader = leader(1, 2);
        leader.set(new byte[0], 0, 0, 1, TimeUnit.SECONDS);

        long termBeforeJoin = leader.currentTerm();

        System.out.println("-- installing new view with previous coordinator back");
        // Include the previous coordinator again.
        cluster.add(address(0), node(0));
        cluster.handleView(createView(view_id++, 1, 2, 0));

        RaftTestUtils.eventually(() -> Objects.equals(raft(0).leader(), leader.getAddress()), 5, TimeUnit.SECONDS);
        assertThat(raft(0).leader()).isEqualTo(leader.getAddress());
        assertThat(raft(0).currentTerm()).isEqualTo(termBeforeJoin);
    }

    private RAFT leader(int ... indexes) {
        return IntStream.of(indexes)
                .mapToObj(this::raft)
                .filter(Objects::nonNull)
                .filter(RAFT::isLeader)
                .findFirst()
                .orElseThrow();
    }

    private void setLog(RAFT raft, int... terms) {
        Log log = raft.log();
        long index = log.lastAppended();
        LogEntries le = new LogEntries();
        for (int term : terms)
            le.add(new LogEntry(term, BUF));
        log.append(index + 1, le);
    }

    private List<Address> clusterLeaders(int ... indexes) {
        return IntStream.of(indexes)
                .mapToObj(this::raft)
                .filter(Objects::nonNull)
                .filter(RAFT::isLeader)
                .map(RAFT::getAddress)
                .collect(Collectors.toList());
    }

    // Checks that there is 1 leader in the cluster and the rest are followers
    protected void assertOneLeader() {
        long count = Arrays.stream(nodes())
                .filter(Objects::nonNull)
                .map(RaftNode::raft)
                .filter(RAFT::isLeader)
                .count();
        assertThat(count).as(this::dumpLeaderAndTerms).isOne();
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.synchronous(true).stateMachine(new DummyStateMachine());
    }
}
