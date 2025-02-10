package org.jgroups.raft.tests;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.View;
import org.jgroups.protocols.raft.AppendEntriesRequest;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.protocols.raft.election.LeaderElected;
import org.jgroups.protocols.raft.election.VoteRequest;
import org.jgroups.protocols.raft.election.VoteResponse;
import org.jgroups.raft.testfwk.BlockingMessageInterceptor;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.util.ResponseCollector;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

/**
 * Uses the synchronous test framework to test {@link org.jgroups.protocols.raft.ELECTION}
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class SyncElectionTests extends BaseRaftElectionTest.ClusterBased<RaftCluster> {

    protected int                view_id=1;


    {
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

    /** Not really an election, as we only have a single member */
    public void testSingletonElection(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster(1);

        assertThat(raft(0).isLeader()).isFalse();
        View view=createView(view_id++, 0);
        cluster.handleView(view);

        assertThat(raft(0).isLeader()).isFalse();
        assertNotElected(2_000, 0);
        assertThat(raft(0).isLeader()).isFalse();

        waitUntilVotingThreadHasStopped();
    }

    public void testElectionWithTwoMembers(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster(2);

        View view=createView(view_id++, 0, 1);
        cluster.handleView(view);

        BooleanSupplier bs = () -> Arrays.stream(nodes())
                .filter(Objects::nonNull)
                .map(RaftNode::raft)
                .anyMatch(RAFT::isLeader);
        assertThat(RaftTestUtils.eventually(bs, 5_000, TimeUnit.MILLISECONDS))
                .as("Should elect a leader")
                .isTrue();

        assertOneLeader();
        System.out.printf("%s\n", print());
        waitUntilVotingThreadHasStopped();
    }

    /** Tests adding a third member. After {A,B} has formed, this should not change anything */
    public void testThirdMember(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster(2);

        View view=createView(view_id++, 0, 1);
        cluster.handleView(view);

        waitUntilLeaderElected(5_000, 0, 1);

        System.out.printf("%s\n", print());

        System.out.println("Adding 3rd member:");

        createCluster();

        view=createView(view_id++, 0, 1, 2);
        cluster.handleView(view);
        assertOneLeader();
        System.out.printf("%s\n", print());
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** Tests A -> ABC */
    public void testGoingFromOneToThree(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster(1);

        View view=createView(view_id++, 0);
        cluster.handleView(view);

        assertNotElected(2_000, 0);
        assertThat(raft(0).isLeader()).isFalse();

        createCluster();
        view=createView(view_id++, 0, 1, 2);
        cluster.handleView(view);

        waitUntilLeaderElected(5_000, 0, 1, 2);

        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** {} -> {ABC} */
    public void testGoingFromZeroToThree(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster();

        View view=createView(view_id++, 0, 1, 2);

        cluster.handleView(view);
        waitUntilLeaderElected(5_000, 0, 1, 2);

        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** Follower is leaving */
    public void testGoingFromThreeToTwo(Class<?> clazz) throws Exception {
        testGoingFromZeroToThree(clazz);

        int follower=findFirst(false);
        close(follower);

        View view=createView(view_id++, 0, 1, 2);
        cluster.handleView(view);

        waitUntilLeaderElected(5_000, 0, 1, 2);

        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    public void testGoingFromThreeToOne(Class<?> clazz) throws Exception {
        testGoingFromZeroToThree(clazz);

        close(findFirst(false));
        close(findFirst(false));

        View view=createView(view_id++, 0, 1, 2);
        cluster.handleView(view);

        waitUntilStepDown(2_000, 0, 1, 2);

        System.out.printf("%s\n", print());
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** ABC (A=leader) -> BC */
    public void testLeaderLeaving(Class<?> clazz) throws Exception {
        testGoingFromZeroToThree(clazz);
        int leader=findFirst(true);
        close(leader);
        View view=createView(view_id++, 0, 1, 2);

        cluster.handleView(view);

        // All nodes but the previous leader.
        int[] indexes = IntStream.range(0, clusterSize).filter(i -> i != leader).toArray();
        waitUntilLeaderElected(5_000, indexes);
        waitUntilVotingThreadStops(5_000, indexes);

        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /** Tests a vote where a non-coord has a longer log */
    public void testVotingFollowerHasLongerLog(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster();

        byte[] data=new byte[4];
        int[] terms={0,1,1,1,2,4,4,5,6,6};
        for(int i=1; i < terms.length; i++) {
            LogEntries entries=new LogEntries().add(new LogEntry(terms[i], data));
            AppendEntriesRequest hdr=new AppendEntriesRequest(address(0),terms[i],i - 1,terms[i - 1],terms[i],0);
            Message msg=new ObjectMessage(address(2), entries).putHeader(raft(2).getId(), hdr);
            cluster.send(msg);
        }

        View view=createView(view_id++, 0, 1, 2);
        cluster.handleView(view);
        waitUntilLeaderElected(10_000, 0, 1, 2);
        System.out.printf("%s\n", print());
        assertOneLeader();

        assertThat(raft(0).isLeader()).isFalse();
        assertThat(raft(2).isLeader()).isTrue();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
        int new_term=terms[terms.length-1]+1;
        assertTerm(new_term, () -> String.format("expected term=%d, actual:\n%s", new_term, print()));
    }

    /** {A}, {B}, {C} -> {A,B,C} */
    public void testMerge(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster();
        View v1,v2,v3;

        node(0).handleView(v1=View.create(address(0), 1, address(0)));
        node(1).handleView(v2=View.create(address(1), 1, address(1)));
        node(2).handleView(v3=View.create(address(2), 1, address(2)));

        assertNotElected(2_000, 0, 1, 2);

        View mv=new MergeView(address(1), 2, Arrays.asList(address(1), address(0), address(2)), Arrays.asList(v2,v1,v3));
        for(RaftNode n: nodes())
            n.handleView(mv);

        waitUntilLeaderElected(5_000, 0, 1, 2);
        System.out.printf("%s\n", print());
        assertOneLeader();
        waitUntilVotingThreadHasStopped();
        assertSameTerm(this::print);
    }

    /**
     * Tests that a member cannot vote for more than 1 leader in the same term. A and B send a vote request to C;
     * only A or B can receive a vote response from C, but not both.
     */
    public void testMultipleVotes(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster();

        BaseElection[] elections = elections();
        Address a = address(0), b = address(1), c = address(2);

        ResponseCollector<VoteResponse> votes_a=elections[0].getVotes(), votes_b=elections[1].getVotes();
        votes_a.reset(a,b,c);
        votes_b.reset(a,b,c);

        System.out.println("-- A and B: sending VoteRequests to C");
        long term_a=raft(0).createNewTerm();
        long term_b=raft(1).createNewTerm();
        elections[0].down(new EmptyMessage(c).putHeader(elections[0].getId(), new VoteRequest(term_a)));
        elections[1].down(new EmptyMessage(c).putHeader(elections[1].getId(), new VoteRequest(term_b)));

        System.out.printf("A: vote responses in term %d: %s\n", term_a, votes_a);
        System.out.printf("B: vote responses in term %d: %s\n", term_b, votes_b);
        int total_rsps=votes_a.numberOfValidResponses() + votes_b.numberOfValidResponses();
        assertThat(total_rsps)
                .as("A and B both received a vote response from C; this is invalid (Raft $3.4)")
                .isLessThanOrEqualTo(1);

        // VoteRequest(term=0): will be dropped by C as C's current_term is 1
        term_b--;
        System.out.printf("-- B: sending VoteRequest to C (term=%d)\n", term_b);
        elections[1].down(new EmptyMessage(c).putHeader(elections[1].getId(), new VoteRequest(term_b)));
        System.out.printf("B: vote responses in term %d: %s\n", term_b, votes_b);
        total_rsps=votes_b.numberOfValidResponses();
        assertThat(total_rsps)
                .as("B should have received no vote response from C")
                .isZero();

        // VoteRequest(term=1): will be dropped by C as C already voted (for A) in term 1
        term_b++;
        System.out.printf("-- B: sending VoteRequest to C (term=%d)\n", term_b);
        elections[1].down(new EmptyMessage(c).putHeader(elections[1].getId(), new VoteRequest(term_b)));
        System.out.printf("B: vote responses in term %d: %s\n", term_b, votes_b);
        total_rsps=votes_b.numberOfValidResponses();
        assertThat(total_rsps)
                .as("B should have received no vote response from C")
                .isZero();

        // VoteRequest(term=2): C will vote for B, as term=2 is new, and C hasn't yet voted for anyone in term 2
        term_b++;
        System.out.printf("-- B: sending VoteRequest to C (term=%d)\n", term_b);
        elections[1].down(new EmptyMessage(c).putHeader(elections[1].getId(), new VoteRequest(term_b)));
        System.out.printf("B: vote responses in term %d: %s\n", term_b, votes_b);
        total_rsps=votes_b.numberOfValidResponses();
        assertThat(total_rsps)
                .as("B should have received a vote response from C")
                .isLessThanOrEqualTo(1);
    }

    public void testIncreasingTermForgetOldLeader(Class<?> ignore) throws Exception {
        withClusterSize(2);
        createCluster();
        View view=createView(view_id++, 0, 1);
        cluster.handleView(view);

        waitUntilVotingThreadStops(5_000, 0, 1);
        assertOneLeader();
        System.out.printf("%s\n", print());
        waitUntilVotingThreadHasStopped();

        int idx = findFirst(true);
        assertThat(raft(idx).isLeader()).isTrue();
        assertThat(raft(idx).role()).isEqualTo("Leader");

        long term = raft(idx).currentTerm();
        assertThat(raft(idx).currentTerm(term + 1)).isOne();
        assertThat(raft(idx).currentTerm()).isEqualTo(term + 1);
        assertThat(raft(idx).isLeader()).isFalse();
        assertThat(raft(idx).role()).isEqualTo("Follower");
    }

    public void testJoinerBeforeSendingElectedMessage(Class<?> ignore) throws Exception {
        withClusterSize(3);
        createCluster();

        View view = createView(view_id++, 0, 1);

        // We intercept the first `LeaderElected` message.
        AtomicBoolean onlyOnce = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m -> {
            for (Map.Entry<Short, Header> h : m.getHeaders().entrySet()) {
                if (h.getValue() instanceof LeaderElected && onlyOnce.getAndSet(false))
                    return true;
            }
            return false;
        });

        // This will install the new view on the nodes and start the election process.
        // The election will proceed as usual, until we intercept the LeaderElected node.
        cluster.handleView(view);

        System.out.println("-- wait command intercept");
        assertThat(RaftTestUtils.eventually(() -> interceptor.numberOfBlockedMessages() > 0, 10, TimeUnit.SECONDS)).isTrue();

        // At this point, the thread is blocked with the LeaderElected message.
        // Node C joins while the message is "in-flight".
        view = createView(view_id++, 0, 1, 2);

        // Add the node again on the cluster. The first view install removed it.
        cluster.add(address(2), node(2));

        System.out.println("-- install new view with node C");
        cluster.handleView(view);

        // View installed, we can release the previous intercepted command.
        interceptor.assertNumberOfBlockedMessages(1);
        interceptor.releaseNext();

        // The new node eventually receives the LeaderElected message.
        System.out.println("-- wait node C receive leader elected");
        assertThat(RaftTestUtils.eventually(() -> raft(2).leader() != null, 10, TimeUnit.SECONDS))
                .isTrue();

        waitUntilVotingThreadHasStopped();
        assertOneLeader();
        interceptor.assertNoBlockedMessages();
    }

    public void testQuorumLostDuringVotingMechanismRuns(Class<?> ignore) throws Throwable {
        withClusterSize(2);
        createCluster();

        // Increase the election timeout for this test.
        for (BaseElection election : elections()) {
            election.voteTimeout(10_000);
        }

        View view = createView(view_id++, 0, 1);
        cluster.async(true);

        // We intercept the `VoteResponse` message.
        AtomicBoolean onlyOnce = new AtomicBoolean(true);
        BlockingMessageInterceptor interceptor = cluster.addCommandInterceptor(m -> {
            // Sent by node `B`.
            if (m.src().equals(node(1).getAddress())) {
                for (Map.Entry<Short, Header> h : m.getHeaders().entrySet()) {
                    if (h.getValue() instanceof VoteResponse && onlyOnce.getAndSet(false))
                        return true;
                }
            }
            return false;
        });

        // This will install the new view on the nodes and start the election process.
        // The election will proceed as usual, until we intercept the VoteResponse from node B.
        cluster.handleView(view);

        System.out.println("-- wait command intercept");
        assertThat(RaftTestUtils.eventually(() -> interceptor.numberOfBlockedMessages() > 0, 10, TimeUnit.SECONDS)).isTrue();

        // This will cause the node to collect all the needed votes it would need to be elected leader.
        // However, the quorum was lost mid-way, the node should never be elected!
        interceptor.assertNumberOfBlockedMessages(1);

        // At this point, the coordinator is executing the vote procedure and collecting the info from other nodes.
        // Node B leaves at this point, which leads to a quorum loss.
        // We release the message concurrently.
        View lostQuorumView = createView(view_id++, 0);
        interceptor.releaseNext();
        cluster.handleView(lostQuorumView);

        // Assert the voting thread will stop running after the quorum loss.
        assertThat(RaftTestUtils.eventually(() -> !election(0).isVotingThreadRunning(), 10, TimeUnit.SECONDS))
                .isTrue();

        // And assert the node is not elected.
        assertThat(raft(0).isLeader()).isFalse();
        assertThat(raft(0).leader()).isNull();
        interceptor.assertNoBlockedMessages();
    }

    protected void assertNotElected(long timeout, int ... indexes) {
        BooleanSupplier bs = () -> Arrays.stream(indexes)
                .mapToObj(this::node)
                .filter(Objects::nonNull)
                .map(RaftNode::raft)
                .anyMatch(RAFT::isLeader);

        assertThat(RaftTestUtils.eventually(bs, timeout, TimeUnit.MILLISECONDS))
                .as("Leader should not be elected")
                .isFalse();
    }

    protected void waitUntilStepDown(long timeout, int ... indexes) {
        BooleanSupplier bs = () -> Arrays.stream(indexes)
                .mapToObj(this::node)
                .filter(Objects::nonNull)
                .map(RaftNode::raft)
                .noneMatch(RAFT::isLeader);

        assertThat(RaftTestUtils.eventually(bs, timeout, TimeUnit.MILLISECONDS))
                .as("Leader should step down")
                .isTrue();
    }

    protected void waitUntilVotingThreadHasStopped() {
        waitUntilVotingThreadStops(5_000, IntStream.range(0, clusterSize).toArray());
    }

    // Checks that there is 1 leader in the cluster and the rest are followers
    protected void assertOneLeader() {
        long count = Arrays.stream(nodes())
                .filter(Objects::nonNull)
                .map(RaftNode::raft)
                .filter(RAFT::isLeader)
                .count();
        assertThat(count).as(this::print).isOne();
    }

    protected void assertSameTerm(Supplier<String> message) {
        long term=-1;
        for (BaseElection election : elections()) {
            if (election == null)
                continue;

            if (term == -1) term=election.raft().currentTerm();
            else assertThat(term).as(message).isEqualTo(election.raft().currentTerm());
        }
    }

    protected void assertTerm(int expected_term, Supplier<String> message) {
        for (BaseElection election : elections()) {
            if (election == null)
                continue;
            assertThat(expected_term).as(message).isEqualTo(election.raft().currentTerm());
        }
    }

    protected int findFirst(boolean leader) {
        for(int i=clusterSize-1; i >= 0; i--) {
            RaftNode node = node(i);
            if (node == null) continue;

            RAFT raft = node.raft();
            if(raft != null && raft.isLeader() == leader)
                return i;
        }
        return -1;
    }

    protected String print() {
        return Stream.of(elections())
                .filter(Objects::nonNull)
                .map(el -> String.format("%s: leader=%s, term=%d", el.getAddress(), el.raft().leader(), el.raft().currentTerm()))
          .collect(Collectors.joining("\n"));
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
