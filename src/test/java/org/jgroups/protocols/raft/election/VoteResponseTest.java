package org.jgroups.protocols.raft.election;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class VoteResponseTest {

    public void testHigherTermWins() {
        VoteResponse lower = new VoteResponse(1, 1, 10);
        VoteResponse higher = new VoteResponse(1, 2, 5);

        assertThat(higher.compareTo(lower)).isPositive();
        assertThat(lower.compareTo(higher)).isNegative();
    }

    public void testSameTermHigherIndexWins() {
        VoteResponse lower = new VoteResponse(1, 1, 5);
        VoteResponse higher = new VoteResponse(1, 1, 10);

        assertThat(higher.compareTo(lower)).isPositive();
        assertThat(lower.compareTo(higher)).isNegative();
    }

    public void testEqualTermAndIndex() {
        VoteResponse a = new VoteResponse(1, 1, 10);
        VoteResponse b = new VoteResponse(1, 1, 10);

        assertThat(a.compareTo(b)).isZero();
        assertThat(b.compareTo(a)).isZero();
    }

    public void testTermTakesPrecedenceOverIndex() {
        // Higher term but lower index still wins.
        VoteResponse higherTerm = new VoteResponse(1, 2, 1);
        VoteResponse higherIndex = new VoteResponse(1, 1, 100);

        assertThat(higherTerm.compareTo(higherIndex)).isPositive();
        assertThat(higherIndex.compareTo(higherTerm)).isNegative();
    }
}
