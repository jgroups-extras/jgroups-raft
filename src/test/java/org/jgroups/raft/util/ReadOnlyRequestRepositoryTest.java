package org.jgroups.raft.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ReadOnlyRequestRepositoryTest {

    public void testReadCompletesWhenMajorityConfirmsItsRound() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = repository(() -> 2);

        long round = repository.register(10, List.of(cf), "leader");
        assertThat(cf).isNotDone();

        repository.confirm(round, "follower-A", 10);
        assertThat(cf).isDone();
    }

    public void testConfirmationOfEarlierRoundIgnoresLaterRead() {
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = repository(() -> 2);

        long firstRound = repository.register(10, List.of(first), "leader");
        long secondRound = repository.register(10, List.of(second), "leader");

        // The response answers a request sent before the second read arrived.
        repository.confirm(firstRound, "follower-A", 10);
        assertThat(first).isDone();
        assertThat(second).as("confirmed by a response to a request sent before the read").isNotDone();

        repository.confirm(secondRound, "follower-A", 10);
        assertThat(second).isDone();
    }

    public void testResponseWithoutRoundConfirmsNothing() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = repository(() -> 2);

        repository.register(10, List.of(cf), "leader");

        // Responses to requests that do not carry a round, e.g., a snapshot install.
        repository.confirm(0, "follower-A", 10);
        assertThat(cf).isNotDone();
    }

    public void testAdvanceAloneDoesNotCompleteRead() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = repository(() -> 3);

        repository.register(10, List.of(cf), "leader");

        // A commit index past the read says nothing about the leader still being the leader.
        repository.advance(11);
        assertThat(cf).isNotDone();
    }

    public void testConfirmedReadWaitsForLeaderStartIndex() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 2)
                .withCommitter(cfs -> cfs.forEach(c -> c.complete(null)))
                .withMinimumIndex(12)
                .build();

        // The new leader's log holds entries up to 12, possibly committed without the leader knowing it yet.
        long round = repository.register(10, List.of(cf), "leader");
        repository.confirm(round, "follower-A", 10);
        assertThat(cf).as("confirmed, but the commit index is behind the leader's start index").isNotDone();

        repository.advance(11);
        assertThat(cf).isNotDone();

        repository.advance(12);
        assertThat(cf).isDone();
    }

    public void testConfirmationAfterCommitReachedCompletesRead() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 2)
                .withCommitter(cfs -> cfs.forEach(c -> c.complete(null)))
                .withMinimumIndex(12)
                .build();

        long round = repository.register(10, List.of(cf), "leader");

        repository.advance(12);
        assertThat(cf).isNotDone();

        // The commit index has already reached the read index, so the confirmation alone completes the read.
        repository.confirm(round, "follower-A", 12);
        assertThat(cf).isDone();
    }

    public void testDuplicateSenderNeverSatisfiesQuorum() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        // 5-node cluster: majority = 3
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = repository(() -> 3);

        // The leader itself counts as vote #1.
        long round = repository.register(10, List.of(cf), "leader");

        // The one minority follower answers twice.
        // Unique voters: {leader, follower-A} = 2, which is below majority=3.
        repository.confirm(round, "follower-A", 10);
        repository.confirm(round, "follower-A", 10);
        repository.advance(10);

        assertThat(cf).isNotDone();
    }

    public void testReadsRegisteredTogetherCompleteTogether() {
        AtomicInteger count = new AtomicInteger();
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 2)
                .withCommitter(cfs -> {
                    assertThat(cfs).hasSize(2);
                    cfs.forEach(c -> c.complete(null));
                    count.incrementAndGet();
                })
                .build();

        // Both reads arrive in the same batch.
        long round = repository.register(10, List.of(first, second), "leader");

        repository.confirm(round, "follower-A", 10);
        assertThat(first).isDone();
        assertThat(second).isDone();
        assertThat(count.get()).isOne();
    }

    public void testConfirmationCoversEarlierRounds() {
        AtomicInteger count = new AtomicInteger();
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 2)
                .withCommitter(cfs -> {
                    assertThat(cfs).hasSize(1);
                    cfs.forEach(c -> c.complete(null));
                    count.incrementAndGet();
                })
                .build();

        repository.register(5, List.of(first), "leader");
        long latest = repository.register(8, List.of(second), "leader");

        // A response to the latest request was sent after both reads arrived.
        repository.confirm(latest, "follower-A", 8);
        assertThat(first).isDone();
        assertThat(second).isDone();
        assertThat(count.get()).isEqualTo(2);
    }

    public void testMajorityChange() {
        AtomicInteger majority = new AtomicInteger(2);
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = repository(majority::get);

        long round = repository.register(10, List.of(cf), "leader");

        // Majority was initiated with 2, but now it updates to 3. This means it needs two confirmations.
        majority.set(3);
        repository.confirm(round, "follower-A", 10);
        assertThat(cf).isNotDone();

        repository.confirm(round, "follower-B", 10);
        assertThat(cf).isDone();
    }

    public void testDestroyCompletePendingRequests() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        AtomicInteger counter = new AtomicInteger();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 20)
                .withDestroyer(cfs -> {
                    assertThat(cfs.size()).isOne();
                    counter.incrementAndGet();
                    cfs.forEach(c -> c.complete(null));
                })
                .build();

        repository.register(10, List.of(cf), "leader");
        assertThat(cf.isDone()).isFalse();

        repository.destroy();
        assertThat(cf.isDone()).isTrue();
        assertThat(counter.get()).isOne();

        // Registering after destroy will automatically invoke the destroyer.
        repository.register(10, List.of(cf), "leader");
        assertThat(counter.get()).isEqualTo(2);
    }

    public void testSingleNodeMajorityCompletesImmediately() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = repository(() -> 1);

        // Single-node: majority = 1.
        // Registering the read should complete immediately without waiting for confirmations.
        repository.register(10, List.of(cf), "leader");
        assertThat(cf.isDone()).isTrue();
    }

    private static ReadOnlyRequestRepository<CompletableFuture<Void>> repository(Supplier<Integer> majority) {
        return ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(majority)
                .withCommitter(cfs -> cfs.forEach(c -> c.complete(null)))
                .build();
    }
}
