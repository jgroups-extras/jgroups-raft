package org.jgroups.protocols.raft.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.state.RaftState.RaftStateMutator;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RaftStateTest {

    private RecordingMutator mutator;
    private RaftState state;

    @BeforeMethod
    public void setUp() {
        mutator = new RecordingMutator();
        state = new RaftState(mutator);
    }

    public void testInitialState() {
        assertThat(state.currentTerm()).isZero();
        assertThat(state.leader()).isNull();
        assertThat(state.votedFor()).isNull();
    }

    public void testLowerTermIsRejected() {
        state.tryAdvanceTerm(5);

        int result = state.tryAdvanceTerm(3);

        assertThat(result).isEqualTo(-1);
        assertThat(state.currentTerm()).isEqualTo(5);
    }

    public void testSameTermNullLeaderIsNoOp() {
        state.tryAdvanceTerm(5);

        int result = state.tryAdvanceTermAndLeader(5, null);

        assertThat(result).isZero();
        assertThat(state.currentTerm()).isEqualTo(5);
    }

    public void testHigherTermClearsLeaderAndVote() {
        Address leader = Util.createRandomAddress("A");
        state.tryAdvanceTermAndLeader(1, leader);
        state.setVotedFor(leader);

        state.tryAdvanceTerm(2);

        assertThat(state.currentTerm()).isEqualTo(2);
        assertThat(state.leader()).isNull();
        assertThat(state.votedFor()).isNull();
    }

    public void testHigherTermSetsLeader() {
        Address leader = Util.createRandomAddress("A");

        state.tryAdvanceTermAndLeader(1, leader);

        assertThat(state.currentTerm()).isEqualTo(1);
        assertThat(state.leader()).isEqualTo(leader);
    }

    public void testAdvanceTermForElection() {
        state.tryAdvanceTerm(5);

        long newTerm = state.advanceTermForElection();

        assertThat(newTerm).isEqualTo(6);
        assertThat(state.currentTerm()).isEqualTo(6);
        assertThat(state.leader()).isNull();
        assertThat(state.votedFor()).isNull();
    }

    public void testStepDownClearsLeaderWithoutChangingTerm() {
        Address leader = Util.createRandomAddress("A");
        state.tryAdvanceTermAndLeader(1, leader);

        state.tryAdvanceTermAndLeader(0, null);

        assertThat(state.leader()).isNull();
        assertThat(state.currentTerm()).isEqualTo(1);
    }

    public void testLeaderNullToNonNull() {
        state.tryAdvanceTerm(1);
        Address leader = Util.createRandomAddress("A");

        state.setLeader(leader);

        assertThat(state.leader()).isEqualTo(leader);
    }

    public void testLeaderToNull() {
        Address leader = Util.createRandomAddress("A");
        state.tryAdvanceTermAndLeader(1, leader);

        state.setLeader(null);

        assertThat(state.leader()).isNull();
    }

    public void testSameLeaderIsAccepted() {
        Address leader = Util.createRandomAddress("A");
        state.tryAdvanceTermAndLeader(1, leader);

        state.setLeader(leader);

        assertThat(state.leader()).isEqualTo(leader);
    }

    public void testLeaderToDifferentLeaderThrows() {
        Address leaderA = Util.createRandomAddress("A");
        Address leaderB = Util.createRandomAddress("B");
        state.tryAdvanceTermAndLeader(1, leaderA);

        assertThatThrownBy(() -> state.setLeader(leaderB))
                .isInstanceOf(IllegalStateException.class);
    }

    public void testVoteNullToNonNull() {
        state.tryAdvanceTerm(1);
        Address candidate = Util.createRandomAddress("A");

        state.setVotedFor(candidate);

        assertThat(state.votedFor()).isEqualTo(candidate);
    }

    public void testSameVoteIsAccepted() {
        state.tryAdvanceTerm(1);
        Address candidate = Util.createRandomAddress("A");
        state.setVotedFor(candidate);

        state.setVotedFor(candidate);

        assertThat(state.votedFor()).isEqualTo(candidate);
    }

    public void testVoteToDifferentVoteThrows() {
        state.tryAdvanceTerm(1);
        Address candidateA = Util.createRandomAddress("A");
        Address candidateB = Util.createRandomAddress("B");
        state.setVotedFor(candidateA);

        assertThatThrownBy(() -> state.setVotedFor(candidateB))
                .isInstanceOf(IllegalStateException.class);
    }

    public void testSetVotedForRejectsNull() {
        assertThatThrownBy(() -> state.setVotedFor(null))
                .isInstanceOf(NullPointerException.class);
    }

    public void testMutatorCalledOnTermAdvance() {
        state.tryAdvanceTerm(5);

        assertThat(mutator.persistedTerms).containsExactly(5L);
    }

    public void testMutatorCalledOnVote() {
        state.tryAdvanceTerm(1);
        Address candidate = Util.createRandomAddress("A");
        mutator.clear();

        state.setVotedFor(candidate);

        assertThat(mutator.persistedVotes).containsExactly(candidate);
    }

    public void testMutatorNotCalledOnRejectedTerm() {
        state.tryAdvanceTerm(5);
        mutator.clear();

        state.tryAdvanceTerm(3);

        assertThat(mutator.persistedTerms).isEmpty();
    }

    public void testMutatorNotCalledOnUnchangedVote() {
        state.tryAdvanceTerm(1);
        Address candidate = Util.createRandomAddress("A");
        state.setVotedFor(candidate);
        mutator.clear();

        state.setVotedFor(candidate);

        assertThat(mutator.persistedVotes).isEmpty();
    }

    public void testMutatorLeaderUpdateCalled() {
        Address leader = Util.createRandomAddress("A");

        state.tryAdvanceTermAndLeader(1, leader);

        assertThat(mutator.leaderUpdates).contains(leader);
    }

    public void testMutatorVoteClearedOnTermAdvance() {
        state.tryAdvanceTerm(1);
        Address candidate = Util.createRandomAddress("A");
        state.setVotedFor(candidate);
        mutator.clear();

        state.tryAdvanceTerm(2);

        assertThat(mutator.persistedVotes).containsExactly((Address) null);
    }

    public void testReloadRestoresState() throws Exception {
        InMemoryLog log = new InMemoryLog();
        log.init("reload-test-" + java.util.UUID.randomUUID(), null);
        Address candidate = Util.createRandomAddress("A");
        log.currentTerm(7);
        log.votedFor(candidate);

        state.reload(log);

        assertThat(state.currentTerm()).isEqualTo(7);
        assertThat(state.votedFor()).isEqualTo(candidate);
        log.close();
    }

    public void testReloadWithNullLogIsNoOp() {
        state.tryAdvanceTerm(5);

        state.reload(null);

        assertThat(state.currentTerm()).isEqualTo(5);
    }

    // --- Concurrent atomicity tests ---

    public void testConcurrentTermAdvancesProduceCorrectTotal() throws Exception {
        int numThreads = 8;
        int advancesPerThread = 200;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < advancesPerThread; i++) {
                        state.advanceTermForElection();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(state.currentTerm()).isEqualTo((long) numThreads * advancesPerThread);
    }

    public void testConcurrentAdvancesNeverExposeStaleLeader() throws Exception {
        Address leader = Util.createRandomAddress("A");
        state.tryAdvanceTermAndLeader(1, leader);

        int numAdvancers = 4;
        int advancesPerThread = 500;
        int numReaders = 4;
        int readsPerThread = 5000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numAdvancers + numReaders);
        AtomicBoolean staleLeaderObserved = new AtomicBoolean(false);

        for (int w = 0; w < numAdvancers; w++) {
            new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < advancesPerThread; i++) {
                        state.advanceTermForElection();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        // No thread sets a leader after the initial setup. Once term advances past 1,
        // leader is never set again — only cleared by advanceTermForElection.
        // Each getter is independently synchronized, but since leader is never re-set,
        // observing term > 1 guarantees leader is already null.
        for (int r = 0; r < numReaders; r++) {
            new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < readsPerThread; i++) {
                        long observedTerm = state.currentTerm();
                        Address observedLeader = state.leader();
                        if (observedTerm > 1 && observedLeader != null) {
                            staleLeaderObserved.set(true);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(staleLeaderObserved.get()).isFalse();
    }

    public void testConcurrentSetLeaderAllowsExactlyOneWinner() throws Exception {
        state.tryAdvanceTerm(1);

        int numThreads = 8;
        CountDownLatch ready = new CountDownLatch(numThreads);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numThreads);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        for (int t = 0; t < numThreads; t++) {
            Address candidate = Util.createRandomAddress(Character.toString('A' + t));
            new Thread(() -> {
                try {
                    ready.countDown();
                    go.await(10, TimeUnit.SECONDS);
                    state.setLeader(candidate);
                    successes.incrementAndGet();
                } catch (IllegalStateException e) {
                    failures.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(successes.get()).isEqualTo(1);
        assertThat(failures.get()).isEqualTo(numThreads - 1);
        assertThat(state.leader()).isNotNull();
    }

    private static class RecordingMutator implements RaftStateMutator {
        final List<Long> persistedTerms = Collections.synchronizedList(new ArrayList<>());
        final List<Address> persistedVotes = Collections.synchronizedList(new ArrayList<>());
        final List<Address> leaderUpdates = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void currentTerm(long term) {
            persistedTerms.add(term);
        }

        @Override
        public void votedFor(Address member) {
            persistedVotes.add(member);
        }

        @Override
        public void onLeaderUpdate(Address member) {
            leaderUpdates.add(member);
        }

        void clear() {
            persistedTerms.clear();
            persistedVotes.clear();
            leaderUpdates.clear();
        }
    }
}
