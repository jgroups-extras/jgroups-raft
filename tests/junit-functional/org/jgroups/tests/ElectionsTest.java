package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.tests.harness.BaseRaftElectionTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public void testElectionWithLongLog(Class<?> ignore) {
        // Let node A be elected the first leader.
        assertLeader(10_000, a.getAddress(), a, b, c);

        // Add the entries, creating longer logs.
        setLog(b, 1, 1, 2);
        setLog(c, 1, 1, 2);

        // Assert that B and C have a longer log.
        long aSize = logLastAppended(a);
        assert aSize < logLastAppended(b) : "A log longer than B";
        assert aSize < logLastAppended(c) : "A log longer than C";

        JChannel coord = findCoord(a, b, c);
        assertThat(coord).isNotNull();

        System.out.printf("\n\n-- starting the voting process on %s:\n", coord.getAddress());
        BaseElection el = election(coord);

        System.out.printf("-- current status: %n%s%n", dumpLeaderAndTerms());

        // Election is not running.
        assertThat(el.isVotingThreadRunning()).isFalse();

        // We start the election process. Eventually, the thread stops after collecting the necessary votes.
        el.startVotingThread();
        waitUntilVotingThreadStops(5_000, 0, 1, 2);

        Address leader = assertLeader(10_000, null, b, c);
        assert leader.equals(b.getAddress()) || leader.equals(c.getAddress()) : dumpLeaderAndTerms();
        assert !leader.equals(a.getAddress());
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
