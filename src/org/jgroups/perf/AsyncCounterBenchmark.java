package org.jgroups.perf;

import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.CompletableFutures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Basic {@link org.jgroups.blocks.atomic.AsyncCounter} benchmark
 */
public class AsyncCounterBenchmark implements CounterBenchmark {

    private List<CompletionStage<Void>> requests;
    private LongSupplier deltaSupplier;
    private int concurrency;
    private AsyncCounter counter;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final LongAdder updates = new LongAdder();
    private final AverageMinMax updateTimes = new AverageMinMax();

    @Override
    public void init(int concurrency, ThreadFactory threadFactory, LongSupplier deltaSupplier, AsyncCounter counter) {
        this.concurrency = concurrency;
        this.deltaSupplier = deltaSupplier;
        this.counter = counter;
        requests = new ArrayList<>(concurrency);
    }

    @Override
    public void start() {
        stop.set(false);
        final long currentTime = System.nanoTime();
        for (int i = 0; i < concurrency; ++i) {
            requests.add(updateCounter(counter, currentTime));
        }
    }

    @Override
    public void stop() {
        stop.set(true);
    }

    @Override
    public void join() throws InterruptedException {
        for (CompletionStage<Void> stage : requests) {
            stage.toCompletableFuture().join();
        }
    }

    @Override
    public long getTotalUpdates() {
        return updates.sum();
    }

    @Override
    public AverageMinMax getResults(boolean printUpdaters, Function<AverageMinMax, String> timePrinter) {
        synchronized (updateTimes) {
            return updateTimes;
        }
    }

    @Override
    public void close() throws Exception {
        stop.set(true);
        requests.clear();
    }

    private void updateTime(long timeNanos) {
        updates.increment();
        synchronized (updateTimes) {
            // AverageMinMax is not thread safe!
            updateTimes.add(timeNanos);
        }
    }

    private CompletionStage<Void> updateCounter(AsyncCounter counter, long start) {
        if (stop.get()) {
            // we don't check the return value
            return CompletableFutures.completedNull();
        }
        return counter.addAndGet(deltaSupplier.getAsLong()).thenCompose(__ -> {
            final long currentTime = System.nanoTime();
            updateTime(currentTime - start);
            return updateCounter(counter, currentTime);
        });
    }
}
