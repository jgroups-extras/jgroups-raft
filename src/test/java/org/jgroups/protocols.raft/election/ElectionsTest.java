package org.jgroups.protocols.raft.election;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.tests.harness.BaseRaftElectionTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

/**
 * Tests elections
 * @author Bela Ban
 * @since 0.2
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class ElectionsTest extends BaseRaftElectionTest.ChannelBased {

    protected JChannel a, b, c;
    protected static final byte[] BUF = {};

    {
        clusterSize = 3;
        recreatePerMethod = true;
    }

    @Override
    protected void afterClusterCreation() {
        a = channel(0);
        b = channel(1);
        c = channel(2);
    }

    /**
     * All members have the same (initial) logs, so any member can be elected as leader
     */
    public void testSimpleElection(Class<?> ignore) {
        assertLeader(10_000, null, a, b, c);
    }


    /**
     * B and C have longer logs than A: one of {B,C} must become coordinator, but *not* A
     */
    public void testElectionWithLongLogTie(Class<?> ignore) {
        testLongestLog(Map.of(b, new int[]{1, 1, 2}, c, new int[]{1, 1, 2}), b.getAddress(), c.getAddress());
    }

    /**
     * B has the longest term in the view {A, B, C}.
     * Node B has to become the leader.
     */
    public void testElectionWithLongLogMiddle(Class<?> ignore) {
        int[] termsB = new int[] { 1, 1, 2, 2 };
        int[] termsC = new int[] { 1, 1, 2 };
        testLongestLog(Map.of(b, termsB, c, termsC), b.getAddress());
    }

    /**
     * C has the longest term in the view {A, B, C}.
     * Node C has to become the leader.
     */
    public void testElectionWithLongLogLast(Class<?> ignore) {
        int[] termsB = new int[] { 1, 1, 2 };
        int[] termsC = new int[] { 1, 1, 2, 2 };
        testLongestLog(Map.of(b, termsB, c, termsC), c.getAddress());
    }

    public void testElectionFollowersHigherTerm(Class<?> ignore) {
        testHigherTerm(Map.of(b, 5, c, 5));
    }

    public void testElectionCoordinatorHigherTerm(Class<?> ignore) {
        testHigherTerm(Map.of(a, 5));
    }

    private void testHigherTerm(Map<JChannel, Integer> terms) {
        assertLeader(10_000, a.getAddress(), a, b, c);
        long aTerm = raft(0).currentTerm();

        for (Map.Entry<JChannel, Integer> entry : terms.entrySet()) {
            setTerm(entry.getKey(), entry.getValue());
        }

        for (JChannel ch : terms.keySet()) {
            assertThat(aTerm)
                    .withFailMessage(this::dumpLeaderAndTerms)
                    .isLessThan(raft(ch).currentTerm());
        }

        JChannel coord = findCoord(a, b, c);
        assertThat(coord).isNotNull();

        System.out.printf("\n\n-- starting the voting process on %s:\n", coord.getAddress());
        BaseElection el = election(coord);

        System.out.printf("-- current status: %n%s%n", dumpLeaderAndTerms());

        // Wait the voting thread to stop.
        // The last step is stopping the voting thread. This means, nodes might receive the leader message,
        // but the voting thread didn't stopped yet.
        // We can use a shorter timeout here.
        waitUntilVotingThreadStops(1_500, 0, 1, 2);

        // We start the election process. Eventually, the thread stops after collecting the necessary votes.
        // Since we test with higher terms, it might take longer to the coordinator to catch up.
        el.startVotingThread();
        waitUntilVotingThreadStops(5_000, 0, 1, 2);

        Address leader = assertLeader(10_000, null, a, b, c);
        assertThat(leader).isEqualTo(coord.getAddress());
    }

    private void testLongestLog(Map<JChannel, int[]> logs, Address ... possibleElected) {
        // Let node A be elected the first leader.
        assertLeader(10_000, a.getAddress(), a, b, c);

        for (Map.Entry<JChannel, int[]> entry : logs.entrySet()) {
            setLog(entry.getKey(), entry.getValue());
        }

        // Assert nodes have longer logs than A.
        long aSize = logLastAppended(a);

        for (JChannel ch : logs.keySet()) {
            assertThat(aSize)
                    .withFailMessage(() -> String.format("Node A has a log longer than %s", ch.getAddress()))
                    .isLessThan(logLastAppended(ch));
        }

        JChannel coord = findCoord(a, b, c);
        assertThat(coord).isNotNull();

        System.out.printf("\n\n-- starting the voting process on %s:\n", coord.getAddress());
        BaseElection el = election(coord);

        System.out.printf("-- current status: %n%s%n", dumpLeaderAndTerms());

        // Wait the voting thread to stop.
        // The last step is stopping the voting thread. This means, nodes might receive the leader message,
        // but the voting thread didn't stopped yet.
        // We can use a shorter timeout here.
        waitUntilVotingThreadStops(1_500, 0, 1, 2);

        // We start the election process. Eventually, the thread stops after collecting the necessary votes.
        el.startVotingThread();
        waitUntilVotingThreadStops(5_000, 0, 1, 2);

        System.out.printf("-- waiting for leader in %s", Arrays.toString(possibleElected));
        Address leader = assertLeader(10_000, null, b, c);
        assertThat(possibleElected).contains(leader);
        assertThat(leader).isNotEqualTo(a.getAddress());
    }

    protected static JChannel findCoord(JChannel... channels) {
        for (JChannel ch : channels)
            if (ch.getView().getCoord().equals(ch.getAddress()))
                return ch;
        return null;
    }

    protected void setLog(JChannel ch, int... terms) {
        RAFT raft = raft(ch);
        Log log = raft.log();
        long index = log.lastAppended();
        LogEntries le = new LogEntries();
        for (int term : terms)
            le.add(new LogEntry(term, BUF));
        log.append(index + 1, le);
    }

    private void setTerm(JChannel ch, int term) {
        RAFT raft = raft(ch);
        raft.setLeaderAndTerm(null, term);
    }

    private long logLastAppended(JChannel ch) {
        RAFT raft = raft(ch);
        Log log = raft.log();
        return log.lastAppended();
    }

    protected static List<Address> leaders(JChannel... channels) {
        List<Address> leaders = new ArrayList<>(channels.length);
        for (JChannel ch : channels) {
            if (RaftTestUtils.isRaftLeader(ch))
                leaders.add(ch.getAddress());
        }
        return leaders;
    }

    /**
     * If expected is null, then any member can be a leader
     */
    protected Address assertLeader(int timeout, Address expected, JChannel... channels) {
        RAFT[] rafts = Arrays.stream(channels)
                .map(this::raft)
                .toArray(RAFT[]::new);
        BaseRaftElectionTest.waitUntilLeaderElected(rafts, timeout);
        List<Address> leaders = leaders(channels);
        assert leaders.size() == 1 : "leaders=" + leaders;
        Address leader = leaders.get(0);
        assert expected == null || leader.equals(expected) : String.format("elected %s instead of %s", leader, expected);
        System.out.println("leader = " + leader);
        return leader;
    }


}
