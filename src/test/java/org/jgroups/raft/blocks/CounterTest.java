package org.jgroups.raft.blocks;

import org.jgroups.Global;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.CounterFunction;
import org.jgroups.blocks.atomic.CounterView;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.Options;
import org.jgroups.raft.tests.harness.BaseRaftChannelTest;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.LongSizeStreamable;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * {@link  AsyncCounter} and {@link  SyncCounter} test.
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CounterTest extends BaseRaftChannelTest {

    protected CounterService service_a, service_b, service_c;

    {
        clusterSize = 3;
    }

    @Override
    protected void afterClusterCreation() {
        service_a = new CounterService(channel(0)).allowDirtyReads(false);
        service_b = new CounterService(channel(1)).allowDirtyReads(false);
        service_c = new CounterService(channel(2)).allowDirtyReads(false);

        RAFT[] rafts = Arrays.stream(channels())
                .map(this::raft)
                .toArray(RAFT[]::new);

        // Need to wait until ALL nodes have the leader.
        // Otherwise, REDIRECT might fail because of not knowing the leader.
        BaseRaftElectionTest.waitUntilAllHaveLeaderElected(rafts, 15_000);
    }

    public void testIncrement() {
        List<RaftSyncCounter> counters = createCounters("increment");

        assertEquals(1, counters.get(0).incrementAndGet());
        assertValues(counters, 1);

        assertEquals(6, counters.get(1).addAndGet(5));
        assertValues(counters, 6);

        assertEquals(11, counters.get(2).addAndGet(5));
        assertValues(counters, 11);
    }

    public void testDecrement() {
        List<RaftSyncCounter> counters = createCounters("decrement");

        assertEquals(-1, counters.get(0).decrementAndGet());
        assertValues(counters, -1);

        assertEquals(-8, counters.get(1).addAndGet(-7));
        assertValues(counters, -8);

        assertEquals(-9, counters.get(2).decrementAndGet());
        assertValues(counters, -9);
    }

    public void testSet() {
        List<RaftSyncCounter> counters = createCounters("set");

        counters.get(0).set(10);
        assertValues(counters, 10);

        counters.get(1).set(15);
        assertValues(counters, 15);

        counters.get(2).set(-10);
        assertValues(counters, -10);
    }

    public void testCompareAndSet() {
        List<RaftSyncCounter> counters = createCounters("casb");

        assertTrue(counters.get(0).compareAndSet(0, 2));
        assertValues(counters, 2);

        assertFalse(counters.get(1).compareAndSet(0, 3));
        assertValues(counters, 2);

        assertTrue(counters.get(2).compareAndSet(2, -2));
        assertValues(counters, -2);
    }

    public void testCompareAndSwap() {
        List<RaftSyncCounter> counters = createCounters("casl");

        assertEquals(0, counters.get(0).compareAndSwap(0, 2));
        assertValues(counters, 2);

        assertEquals(2, counters.get(1).compareAndSwap(0, 3));
        assertValues(counters, 2);

        assertEquals(2, counters.get(2).compareAndSwap(2, -2));
        assertValues(counters, -2);
    }

    public void testIgnoreReturnValue() throws Exception {
        List<RaftSyncCounter> counters=createCounters("ignore");
        RaftSyncCounter counter=counters.get(1);
        long ret=counter.incrementAndGet();
        assert ret == 1;

        counter=counter.withOptions(Options.create(true));
        ret=counter.incrementAndGet();
        assert ret == 0;

        ret=counter.addAndGet(10);
        assert ret == 0;
        ret=counter.get();
        assert ret == 12;

        final RaftSyncCounter rfc = counter;
        Util.waitUntil(5_000, 250, () -> rfc.getLocal() == 12);
        ret=counter.getLocal();
        assert ret == 12;

        boolean rc=counter.compareAndSet(12, 15);
        assert !rc;

        ret=counter.get();
        assert ret == 15;

        counter.set(20);
        ret=counter.get();
        assert ret == 20;

        RaftAsyncCounter ctr=counter.async();
        CompletionStage<Long> f=ctr.addAndGet(5);
        Long val=CompletableFutures.join(f);
        assert val == null;
        f=ctr.get();
        ret=CompletableFutures.join(f);
        assert ret == 25;
        Util.waitUntil(5_000, 250, () -> rfc.getLocal() == 25);
        ret=ctr.getLocal();
        assert ret == 25;
        f=ctr.incrementAndGet();
        val=CompletableFutures.join(f);
        assert val == null;
        ctr.set(30);
        assert CompletableFutures.join(ctr.get()) == 30;

        counter=counter.withOptions(Options.create(false));
        ret=counter.decrementAndGet();
        assert ret == 29;
    }

    public void testChainAddAndGet() {
        List<AsyncCounter> counters = createAsyncCounters("chain-add-and-get");

        CompletionStage<Long> stage = counters.get(0).addAndGet(5)
                .thenCompose(value -> counters.get(0).addAndGet(value));
        stage.thenAccept(value -> assertEquals(10L, (long) value)).toCompletableFuture().join();

        List<CompletionStage<Boolean>> checkValueStage = new ArrayList<>(counters.size());
        Function<Long, Boolean> isTen = value -> value == 10;
        for (AsyncCounter c : counters) {
            checkValueStage.add(c.get().thenApply(isTen));
        }
        for (CompletionStage<Boolean> c : checkValueStage) {
            assertTrue(c.toCompletableFuture().join());
        }
        assertAsyncValues(counters, 10);
    }

    public void testCombineCounters() {
        List<AsyncCounter> counters1 = createAsyncCounters("combine-1");
        List<AsyncCounter> counters2 = createAsyncCounters("combine-2");

        long delta1 = Util.random(100);
        long delta2 = Util.random(100);

        CompletionStage<Long> stage1 = counters1.get(0).addAndGet(delta1);
        CompletionStage<Long> stage2 = counters2.get(1).addAndGet(delta2);

        stage1.thenCombine(stage2, Math::max)
                .thenAccept(value -> assertEquals(Math.max(delta1, delta2), (long) value))
                .toCompletableFuture().join();

        assertAsyncValues(counters1, delta1);
        assertAsyncValues(counters2, delta2);
    }

    public void testCompareAndSwapChained() {
        List<AsyncCounter> counters = createAsyncCounters("cas-chained");

        final long initialValue = 100;
        final long finalValue = 10;

        counters.get(0).set(initialValue).toCompletableFuture().join();

        AtomicLong rv = new AtomicLong();
        boolean result = counters.get(0).compareAndSwap(1, finalValue)
                .thenCompose(rValue -> {
                    rv.set(rValue);
                    return counters.get(0).compareAndSwap(rValue, finalValue);
                })
                .thenApply(value -> value == initialValue)
                .toCompletableFuture()
                .join();

        assertTrue(result);
        assertEquals(initialValue, rv.longValue());
        assertAsyncValues(counters, finalValue);
    }

    public void testCompareAndSetChained() {
        List<AsyncCounter> counters = createAsyncCounters("casb-chained");
        final AsyncCounter counter = counters.get(0);

        final long initialValue = 100;
        final long finalValue = 10;

        counter.set(initialValue).toCompletableFuture().join();

        boolean result = counter.compareAndSet(1, finalValue)
                .thenCompose(success -> {
                    if (success) {
                        // should not reach here
                        return CompletableFutures.completedFalse();
                    }
                    return counter.get().thenCompose(value -> counter.compareAndSet(value, finalValue));
                })
                .toCompletableFuture()
                .join();

        assertTrue(result);
        assertAsyncValues(counters, finalValue);
    }

    public void testAsyncIncrementPerf() {
        List<AsyncCounter> counters = createAsyncCounters("async-perf-1");
        final AsyncCounter counter = counters.get(0);
        final long maxValue = 10_000;

        CompletableFuture<Long> stage = CompletableFutures.completedNull();
        Function<Long, CompletionStage<Long>> increment = __ -> counter.incrementAndGet();

        long start = System.currentTimeMillis();

        for (int i = 0; i < maxValue; ++i) {
            stage = stage.thenCompose(increment);
        }

        long loopEnd = System.currentTimeMillis() - start;

        stageEquals(maxValue, stage);

        long waitEnd = System.currentTimeMillis() - start;

        LOGGER.info("async perf: val={}, loop time={} ms, total time={}", counter.get().toCompletableFuture().join(), loopEnd, waitEnd);
        assertAsyncValues(counters, maxValue);
    }

    public void testSyncIncrementPerf() {
        List<RaftSyncCounter> counters = createCounters("sync-perf-1");
        final SyncCounter counter = counters.get(0);
        final long maxValue = 10_000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < maxValue; ++i) {
            counter.incrementAndGet();
        }
        long time = System.currentTimeMillis() - start;

        LOGGER.info("sync perf: val={}, time={} ms", counter.get(), time);
        assertValues(counters, maxValue);
    }

    public void testConcurrentCas() {
        List<AsyncCounter> counters = createAsyncCounters("ccas");
        final long maxValue = 10_000;

        List<CompletionStage<Long>> results = new ArrayList<>(counters.size());
        List<AtomicInteger> successes = new ArrayList<>(counters.size());

        long start = System.currentTimeMillis();

        for (AsyncCounter c : counters) {
            AtomicInteger s = new AtomicInteger();
            successes.add(s);
            results.add(compareAndSwap(c, 0, 1, maxValue, s));
        }

        long loopEnd = System.currentTimeMillis() - start;

        for (CompletionStage<Long> c : results) {
            stageEquals(maxValue, c);
        }

        long waitEnd = System.currentTimeMillis() - start;

        LOGGER.info("cas async perf: val={}, loop time={} ms, total time={}", counters.get(0).get().toCompletableFuture().join(), loopEnd, waitEnd);
        assertAsyncValues(counters, maxValue);

        long casCount = 0;
        for (int i = 0; i < successes.size(); ++i) {
            LOGGER.info("cas results for node {}: {} CAS succeed", i, successes.get(i).intValue());
            casCount += successes.get(i).longValue();
        }
        assertEquals(maxValue, casCount);
    }

    public void testDelete() throws Exception {
        List<AsyncCounter> counters = createAsyncCounters("to-delete");
        for (AsyncCounter counter : counters) {
            assertEquals(0, counter.sync().get());
        }

        assert counters.size() == 3 && counters.stream().allMatch(Objects::nonNull);

        // We create the counter from each server, but the request is redirected to the leader.
        // In some occasions, with 3 nodes, we can end up committing only on the same 2 of the three in all requests.
        // When this happens, we retrieve the printCounters locally, without a consensus read, so the counter is not locally created yet.
        BooleanSupplier bs = () -> Stream.of(service_a, service_b, service_c)
                .allMatch(service -> service.printCounters().contains("to-delete"));
        Supplier<String> message = () -> Stream.of(service_a, service_b, service_c)
                .map(service -> String.format("%s: %s", service.raftId(), service.printCounters()))
                .collect(Collectors.joining(System.lineSeparator()));
        assertThat(eventually(bs, 10, TimeUnit.SECONDS))
                .as(message)
                .isTrue();

        // blocks until majority
        service_a.deleteCounter("to-delete");

        // wait at most for 10 seconds
        // delete may take a while to replicate
        boolean counterRemoved = false;
        String raftMemberWithCounter = null;
        for (int i = 0; i< 10 && !counterRemoved; ++i) {
            counterRemoved = true;
            raftMemberWithCounter = null;
            for (CounterService service : Arrays.asList(service_a, service_b, service_c)) {
                if (service.printCounters().contains("to-delete")) {
                    counterRemoved = false;
                    raftMemberWithCounter = service.raftId();
                }
            }
            if (!counterRemoved) {
                Thread.sleep(1000);
            }
        }
        assertTrue("Counter exists in " + raftMemberWithCounter, counterRemoved);
    }

    public void testIncrementUsingFunction() {
        // copied from JGroups testsuite
        List<RaftSyncCounter> counters = createCounters("increment-function");
        assertEquals(1, counters.get(0).incrementAndGet());
        assertEquals(1, counters.get(1).update(new GetAndAddFunction(1)).getAsLong());
        assertEquals(2, counters.get(1).update(new GetAndAddFunction(5)).getAsLong());
        assertValues(counters, 7);
    }

    public void testComplexFunction() {
        List<RaftSyncCounter> counters = createCounters("complex-function");
        assertEquals(0, counters.get(0).get());
        AddWithLimitResult res = counters.get(1).update(new AddWithLimitFunction(2, 10));
        assertEquals(2, res.result);
        assertFalse(res.limitReached);
        assertValues(counters, 2);

        res = counters.get(2).update(new AddWithLimitFunction(8, 10));
        assertEquals(10, res.result);
        assertFalse(res.limitReached);
        assertValues(counters, 10);

        res = counters.get(2).update(new AddWithLimitFunction(1, 10));
        assertEquals(10, res.result);
        assertTrue(res.limitReached);
        assertValues(counters, 10);

        counters.get(2).set(0);
        assertValues(counters, 0);

        res = counters.get(1).update(new AddWithLimitFunction(20, 10));
        assertEquals(10, res.result);
        assertTrue(res.limitReached);
        assertValues(counters, 10);
    }

    public void testFunctionWithoutReturnValue() {
        List<RaftSyncCounter> counters = createCounters("function-without-retval");
        AddWithLimitResult res = counters.get(1)
                .withOptions(Options.create(true))
                .update(new AddWithLimitFunction(2, 10));
        assertNull(res);
        assertValues(counters, 2);
    }

    private static CompletionStage<Long> compareAndSwap(AsyncCounter counter, long expected, long update, long maxValue, AtomicInteger successes) {
        return counter.compareAndSwap(expected, update)
                .thenCompose(value -> {
                    // cas is successful if return value is equals to expected
                    if (value == expected) {
                        successes.incrementAndGet();
                    }
                    return value < maxValue ?
                            compareAndSwap(counter, value, value + 1, maxValue, successes) :
                            CompletableFuture.completedFuture(value);
                });
    }

    private static void stageEquals(long value, CompletionStage<Long> stage) {
        assertEquals(value, (long) stage.toCompletableFuture().join());
    }

    private static void assertValues(List<RaftSyncCounter> counters, long expectedValue) {
        for (SyncCounter counter : counters) {
            assertEquals(expectedValue, counter.get());
        }
    }

    private static void assertAsyncValues(List<AsyncCounter> counters, long expectedValue) {
        for (AsyncCounter counter : counters) {
            stageEquals(expectedValue, counter.get());
        }
    }

    private List<RaftSyncCounter> createCounters(String name) {
        return Stream.of(service_a, service_b, service_c)
                .map(counterService -> createCounter(name, counterService))
                .map(RaftAsyncCounter::sync)
                .collect(Collectors.toList());
    }

    private List<AsyncCounter> createAsyncCounters(String name) {
        return Stream.of(service_a, service_b, service_c)
                .map(counterService -> createCounter(name, counterService))
                .collect(Collectors.toList());
    }

    private static RaftAsyncCounter createCounter(String name, CounterService counterService) {
        return CompletableFutures.join(counterService.getOrCreateAsyncCounter(name, 0));
    }

    public static class GetAndAddFunction implements CounterFunction<LongSizeStreamable>, SizeStreamable {

        long delta;

        // for unmarshalling
        @SuppressWarnings("unused")
        public GetAndAddFunction() {}

        public GetAndAddFunction(long delta) {
            this.delta = delta;
        }

        @Override
        public LongSizeStreamable apply(CounterView counterView) {
            long ret = counterView.get();
            counterView.set(ret + delta);
            return new LongSizeStreamable(ret);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeLong(delta);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            delta = in.readLong();
        }

        @Override
        public int serializedSize() {
            return Long.BYTES;
        }
    }

    public static class AddWithLimitFunction implements CounterFunction<AddWithLimitResult>, SizeStreamable {

        long delta;
        long limit;

        // for unmarshalling
        @SuppressWarnings("unused")
        public AddWithLimitFunction() {
        }

        public AddWithLimitFunction(long delta, long limit) {
            this.delta = delta;
            this.limit = limit;
        }

        @Override
        public AddWithLimitResult apply(CounterView counterView) {
            long newValue = counterView.get() + delta;
            if (newValue > limit) {
                counterView.set(limit);
                return new AddWithLimitResult(limit, true);
            } else {
                counterView.set(newValue);
                return new AddWithLimitResult(newValue, false);
            }
        }

        @Override
        public int serializedSize() {
            return Long.BYTES * 2;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeLong(delta);
            out.writeLong(limit);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            delta = in.readLong();
            limit = in.readLong();
        }
    }

    public static class AddWithLimitResult implements SizeStreamable {

        long result;
        boolean limitReached;

        // for unmarshalling
        @SuppressWarnings("unused")
        public AddWithLimitResult() {}

        public AddWithLimitResult(long result, boolean limitReached) {
            this.result = result;
            this.limitReached = limitReached;
        }

        @Override
        public int serializedSize() {
            return Long.BYTES + 1;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeLong(result);
            out.writeBoolean(limitReached);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            result = in.readLong();
            limitReached = in.readBoolean();
        }
    }
}
