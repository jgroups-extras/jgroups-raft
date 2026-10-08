package org.jgroups.raft.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ReadOnlyRequestRepositoryTest {

    public void testDuplicateSenderNeverSatisfiesQuorum() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        // 5-node cluster: majority = 3
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository =
                ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 3)
                        .withCommitter(cfs -> cfs.forEach(c -> c.complete(null)))
                        .build();

        // Leader registers the read at index 10; the leader itself counts as vote #1.
        repository.register(10, cf, "leader");
        assertThat(cf.isDone()).isFalse();

        // The one minority follower sends two heartbeat responses (as happens in production).
        // Unique voters: {leader, follower-A} = 2, which is below majority=3.
        repository.commit(10, "follower-A");
        repository.commit(10, "follower-A");

        // Advance at same index should not complete uncommitted entries.
        repository.advance(10);

        // Must NOT complete: only 2 unique voters, majority requires 3.
        assertThat(cf.isDone()).isFalse();
    }

    public void testMajorityReached() {
        AtomicInteger count = new AtomicInteger();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 2)
                .withCommitter(cfs -> {
                    assertThat(cfs.size()).isOne();
                    cfs.forEach(c -> c.complete(null));
                    count.incrementAndGet();
                })
                .build();

        repository.register(10, cf, "leader");
        assertThat(cf.isDone()).isFalse();
        assertThat(count.get()).isZero();

        // Lower commit index doesn't affect.
        repository.commit(9, "follower-A");
        assertThat(cf.isDone()).isFalse();
        assertThat(count.get()).isZero();

        repository.commit(10, "follower-A");
        assertThat(cf.isDone()).isTrue();
        assertThat(count.get()).isOne();

        repository.commit(10, "follower-B");
        assertThat(count.get()).isOne();
    }

    public void testCommitAdvance() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 20)
                .withCommitter(cfs -> {
                    assertThat(cfs.size()).isOne();
                    cfs.forEach(c -> c.complete(null));
                })
                .build();

        repository.register(10, cf, "leader");
        assertThat(cf.isDone()).isFalse();

        repository.commit(10, "follower-A");
        assertThat(cf.isDone()).isFalse();

        repository.advance(11);
        assertThat(cf.isDone()).isTrue();
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

        repository.register(10, cf, "leader");
        assertThat(cf.isDone()).isFalse();

        repository.destroy();
        assertThat(cf.isDone()).isTrue();
        assertThat(counter.get()).isOne();

        // Registering after destroy will automatically invoke the destroyer.
        repository.register(10, cf, "leader");
        assertThat(counter.get()).isEqualTo(2);
    }

    public void testMajorityChange() {
        AtomicInteger majority = new AtomicInteger(2);
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(majority::get)
                .withCommitter(cfs -> {
                    assertThat(cfs.size()).isOne();
                    cfs.forEach(c -> c.complete(null));
                })
                .build();

        repository.register(10, cf, "leader");

        // Majority was initiated with 2, but now it updates to 3. This means it needs two commits.
        majority.set(3);
        repository.commit(10, "follower-A");
        assertThat(cf.isDone()).isFalse();

        repository.commit(10, "follower-B");
        assertThat(cf.isDone()).isTrue();
    }

    public void testCommitIsUpTo() {
        AtomicInteger count = new AtomicInteger();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ReadOnlyRequestRepository<CompletableFuture<Void>> repository = ReadOnlyRequestRepository.<CompletableFuture<Void>>builder(() -> 2)
                .withCommitter(cfs -> {
                    assertThat(cfs.size()).isOne();
                    cfs.forEach(c -> c.complete(null));
                    count.incrementAndGet();
                })
                .build();

        assertThat(count.get()).isZero();
        // We register the request at index 5 and 8, but will commit with higher commits.
        repository.register(5, cf, "leader");
        repository.register(8, cf, "leader");

        repository.commit(10, "follower-A");
        assertThat(cf.isDone()).isTrue();

        // Since it committed up-to 10, listener is invoked for 5 and 8.
        assertThat(count.get()).isEqualTo(2);
    }
}
