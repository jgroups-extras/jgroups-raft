package org.jgroups.protocols.raft.election;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.UUID;

import org.testng.annotations.Test;

/**
 * Tests for {@link BaseElection#determineLeader(Address)} exclusion logic.
 *
 * <p>
 * Uses a standalone {@link ELECTION} instance with manually configured view and votes
 * to verify the runner-up selection when the highest node is excluded.
 * </p>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class DetermineLeaderExclusionTest {

    private final Address a = UUID.randomUUID();
    private final Address b = UUID.randomUUID();
    private final Address c = UUID.randomUUID();

    /**
     * No exclusion -- returns the highest node (first in view on tie).
     */
    public void testNoExclusion() {
        ELECTION election = createElection(a, b, c);
        addVotes(election, 1, 10);

        Address leader = election.determineLeader(null);
        // All tie, first in view wins.
        assertThat(leader).isEqualTo(a);
    }

    /**
     * Excluded node ties with others -- returns runner-up.
     */
    public void testExcludedNodeTiesReturnsRunnerUp() {
        ELECTION election = createElection(a, b, c);
        addVotes(election, 1, 10);

        // Exclude A (first in view, would normally win the tie).
        Address leader = election.determineLeader(a);
        assertThat(leader).isEqualTo(b);
    }

    /**
     * Excluded node is strictly ahead -- returns null to signal retry.
     */
    public void testExcludedNodeStrictlyAheadReturnsNull() {
        ELECTION election = createElection(a, b, c);
        ResponseCollector<VoteResponse> votes = election.getVotes();
        votes.reset(a, b, c);

        // A has a longer log than B and C.
        votes.add(a, new VoteResponse(1, 1, 20));
        votes.add(b, new VoteResponse(1, 1, 10));
        votes.add(c, new VoteResponse(1, 1, 10));

        Address leader = election.determineLeader(a);
        assertThat(leader).isNull();
    }

    /**
     * Excluded node is NOT the highest -- exclusion is irrelevant, returns the actual highest.
     */
    public void testExcludedNodeNotHighest() {
        ELECTION election = createElection(a, b, c);
        ResponseCollector<VoteResponse> votes = election.getVotes();
        votes.reset(a, b, c);

        // C has the highest log.
        votes.add(a, new VoteResponse(1, 1, 5));
        votes.add(b, new VoteResponse(1, 1, 5));
        votes.add(c, new VoteResponse(1, 1, 20));

        // Exclude A, but C is highest -- A exclusion is irrelevant.
        Address leader = election.determineLeader(a);
        assertThat(leader).isEqualTo(c);
    }

    /**
     * Excluded node has higher term -- returns null even if others have higher index.
     */
    public void testExcludedNodeHigherTermReturnsNull() {
        ELECTION election = createElection(a, b, c);
        ResponseCollector<VoteResponse> votes = election.getVotes();
        votes.reset(a, b, c);

        // A has higher term but lower index.
        votes.add(a, new VoteResponse(1, 2, 5));
        votes.add(b, new VoteResponse(1, 1, 100));
        votes.add(c, new VoteResponse(1, 1, 100));

        Address leader = election.determineLeader(a);
        assertThat(leader).isNull();
    }

    /**
     * Only two nodes, excluded ties with the other -- returns the other.
     */
    public void testTwoNodesExcludedTies() {
        ELECTION election = createElection(a, b);
        addVotes(election, 1, 10, a, b);

        Address leader = election.determineLeader(a);
        assertThat(leader).isEqualTo(b);
    }

    /**
     * Missing vote from a node -- only nodes with responses are considered.
     */
    public void testMissingVote() {
        ELECTION election = createElection(a, b, c);
        ResponseCollector<VoteResponse> votes = election.getVotes();
        votes.reset(a, b, c);

        // C did not respond.
        votes.add(a, new VoteResponse(1, 1, 10));
        votes.add(b, new VoteResponse(1, 1, 10));

        Address leader = election.determineLeader(a);
        assertThat(leader).isEqualTo(b);
    }

    private ELECTION createElection(Address... members) {
        ELECTION election = new ELECTION();
        election.view = View.create(members[0], 1, members);
        return election;
    }

    private void addVotes(ELECTION election, long term, long index) {
        addVotes(election, term, index, a, b, c);
    }

    private void addVotes(ELECTION election, long term, long index, Address... members) {
        ResponseCollector<VoteResponse> votes = election.getVotes();
        votes.reset(members);
        for (Address mbr : members) {
            votes.add(mbr, new VoteResponse(1, term, index));
        }
    }
}
