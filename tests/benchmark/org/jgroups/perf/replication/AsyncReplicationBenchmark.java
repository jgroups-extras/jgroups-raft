package org.jgroups.perf.replication;

import org.jgroups.perf.harness.RaftBenchmark;
import org.jgroups.perf.counter.HistogramUtil;
import org.jgroups.raft.Settable;
import org.jgroups.util.CompletableFutures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

/**
 * A benchmark for asynchronous APIs.
 * <p>
 * Run multiple asynchronous requests in parallel. A new request initiates as soon as a request finishes. Utilize the
 * {@link org.jgroups.raft.RaftHandle#setAsync(byte[], int, int)} API.
 * </p>
 *
 */
class AsyncReplicationBenchmark implements RaftBenchmark {

    private final int concurrency;
    private final byte[] payload;
    private final Settable settable;
    private final ThreadFactory tf;
    private final List<CompletionStage<Void>> requests;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final LongAdder updates = new LongAdder();
    private final AtomicHistogram histogram = HistogramUtil.createAtomic();


    public AsyncReplicationBenchmark(int concurrency, Settable settable, byte[] payload, ThreadFactory tf) {
        this.concurrency = concurrency;
        this.payload = payload;
        this.settable = settable;
        this.tf = tf;
        this.requests = new ArrayList<>(concurrency);
    }

    @Override
    public void start() {
        stop.set(false);
        for (int i = 0; i < concurrency; i++) {
            requests.add(perform());
        }
    }

    @Override
    public void stop() {
        stop.set(true);
    }

    @Override
    public void join() throws InterruptedException {
        for (CompletionStage<Void> request : requests) {
            request.toCompletableFuture().join();
        }
    }

    @Override
    public long getTotalUpdates() {
        return updates.sum();
    }

    @Override
    public Histogram getResults(boolean printUpdaters, Function<AbstractHistogram, String> timePrinter) {
        return histogram;
    }

    @Override
    public void close() throws Exception {
        stop.set(true);
        requests.clear();
    }

    private void updateTime(long timeNanos) {
        updates.increment();
        histogram.recordValue(timeNanos);
    }

    private CompletionStage<Void> perform() {
        if (stop.get())
            return CompletableFutures.completedNull();

        CompletableFuture<Void> cf = new CompletableFuture<>();
        perform(cf, execute(), System.nanoTime());
        return cf;
    }

    private void perform(CompletableFuture<Void> cf, CompletionStage<?> prev, long start) {
        if (stop.get()) {
            cf.complete(null);
            return;
        }

        prev.whenComplete((ignoreV, ignoreT) -> {
            long currentTime = System.nanoTime();
            updateTime(currentTime - start);
            perform(cf, execute(), System.nanoTime());
        });
    }

    private CompletableFuture<?> execute() {
        try {
            return settable.setAsync(payload, 0, payload.length);
        } catch (Exception e) {
            if (!stop.get())
                e.printStackTrace(System.err);
        }
        return CompletableFutures.completedNull();
    }
}
