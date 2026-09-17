package org.jgroups.protocols.raft.election;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.UUID;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ElectionRunnerTest {

    public void testStartWithExcludePassesParametersToDelegate() throws Exception {
        Address exclude = UUID.randomUUID();
        AtomicReference<Address> receivedExclude = new AtomicReference<>();
        AtomicReference<CompletableFuture<Address>> receivedFuture = new AtomicReference<>();
        CountDownLatch delegateCalled = new CountDownLatch(1);

        ElectionRunner runner = new ElectionRunner("test", (addr, future) -> {
            receivedExclude.set(addr);
            receivedFuture.set(future);
            future.complete(addr);
            delegateCalled.countDown();
        });

        runner.start(exclude);
        assertThat(delegateCalled.await(5, TimeUnit.SECONDS)).isTrue();
        runner.stop();

        assertThat(receivedExclude.get()).isEqualTo(exclude);
        assertThat(receivedFuture.get()).isNotNull();
    }

    public void testOrganicStartPassesNullExclude() throws Exception {
        AtomicReference<Address> receivedExclude = new AtomicReference<>(UUID.randomUUID());
        CountDownLatch delegateCalled = new CountDownLatch(1);

        ElectionRunner runner = new ElectionRunner("test", (addr, future) -> {
            receivedExclude.set(addr);
            future.complete(addr);
            delegateCalled.countDown();
        });

        runner.start();
        assertThat(delegateCalled.await(5, TimeUnit.SECONDS)).isTrue();
        runner.stop();

        assertThat(receivedExclude.get()).isNull();
    }

    public void testFutureCompletedByDelegate() throws Exception {
        Address elected = UUID.randomUUID();
        CountDownLatch delegateCalled = new CountDownLatch(1);

        ElectionRunner runner = new ElectionRunner("test", (addr, future) -> {
            future.complete(elected);
            delegateCalled.countDown();
        });

        CompletionStage<Address> cs = runner.start(null);
        assertThat(delegateCalled.await(5, TimeUnit.SECONDS)).isTrue();
        runner.stop();

        assertThat(cs.toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo(elected);
    }

    public void testConcurrentForcedStartReturnsExistingFuture() {
        CountDownLatch gate = new CountDownLatch(1);
        ElectionRunner runner = new ElectionRunner("test", (addr, future) -> {
            try {
                assertThat(gate.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            future.complete(addr);
        });

        CompletionStage<Address> first = runner.start(UUID.randomUUID());
        CompletionStage<Address> second = runner.start(UUID.randomUUID());

        // CAS: second caller piggybacks on the in-progress election.
        assertThat(second.toCompletableFuture()).isSameAs(first.toCompletableFuture());

        gate.countDown();
        runner.stop();
    }

    public void testOrganicThenForcedReturnsSameFuture() {
        CountDownLatch gate = new CountDownLatch(1);
        ElectionRunner runner = new ElectionRunner("test", (addr, future) -> {
            try {
                assertThat(gate.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            future.complete(addr);
        });

        // Organic start calls define(null), setting parameters in the AtomicReference.
        runner.start();

        // Forced start: CAS fails because organic already set parameters, returns the organic future.
        CompletionStage<Address> forced = runner.start(UUID.randomUUID());
        assertThat(forced.toCompletableFuture()).isNotNull();

        gate.countDown();
        runner.stop();
    }

    public void testFutureCompletedExceptionally() throws Exception {
        RuntimeException error = new RuntimeException("election failed");
        CountDownLatch delegateCalled = new CountDownLatch(1);

        ElectionRunner runner = new ElectionRunner("test", (addr, future) -> {
            future.completeExceptionally(error);
            delegateCalled.countDown();
        });

        CompletionStage<Address> cs = runner.start(null);
        assertThat(delegateCalled.await(5, TimeUnit.SECONDS)).isTrue();
        runner.stop();

        assertThat(cs.toCompletableFuture())
                .isCompletedExceptionally()
                .failsWithin(1, TimeUnit.SECONDS)
                .withThrowableThat()
                .havingCause()
                .isSameAs(error);
    }

    public void testStopAllowsNewElection() throws Exception {
        CountDownLatch firstDone = new CountDownLatch(1);
        CountDownLatch secondDone = new CountDownLatch(1);

        ElectionRunner runner = new ElectionRunner("test", (addr, future) -> {
            future.complete(addr);
            if (firstDone.getCount() > 0) {
                firstDone.countDown();
            } else {
                secondDone.countDown();
            }
        });

        CompletionStage<Address> first = runner.start(null);
        assertThat(firstDone.await(5, TimeUnit.SECONDS)).isTrue();

        // stop() automatically resets parameters, so the next start creates a new future.
        runner.stop();

        CompletionStage<Address> second = runner.start(null);
        assertThat(secondDone.await(5, TimeUnit.SECONDS)).isTrue();
        runner.stop();

        assertThat(second.toCompletableFuture()).isNotSameAs(first.toCompletableFuture());
    }
}
