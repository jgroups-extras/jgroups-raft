package org.jgroups.perf;

import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Basic {@link org.jgroups.blocks.atomic.AsyncCounter} benchmark
 */
public class AsyncCounterBenchmark implements CounterBenchmark, Runnable {

    private LongSupplier deltaSupplier;
    private int concurrency;
    private AsyncCounter counter;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final LongAdder updates = new LongAdder();
    private final LongAdder requests = new LongAdder();
    private final AverageMinMax updateTimes = new AverageMinMax();
    private Thread updaterThread;
    private final BlockingQueue<Long> queue = new LinkedBlockingQueue<>();

    @Override
    public void init(int concurrency, ThreadFactory threadFactory, LongSupplier deltaSupplier, AsyncCounter counter) {
        this.concurrency = concurrency;
        this.deltaSupplier = deltaSupplier;
        this.counter = counter;
        updaterThread = threadFactory.newThread(this);
        updaterThread.setName("async-updater");
    }

    @Override
    public void start() {
        stop.set(false);
        updaterThread.start();
        //final long currentTime = System.nanoTime();
        for (int i = 0; i < concurrency; ++i) {
            updateCounter(counter);
        }
    }

    @Override
    public void stop() {
        stop.set(true);
        queue.offer(0L);
    }

    @Override
    public void join() throws InterruptedException {
        updaterThread.join();
        while (requests.sum() != updates.sum()) {
            Util.sleep(10);
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
        updaterThread.interrupt();
        updaterThread.join();
    }

    private void updateTime(long timeNanos) {
        updates.increment();
        synchronized (updateTimes) {
            // AverageMinMax is not thread safe!
            updateTimes.add(timeNanos);
        }
    }

    private void updateCounter(AsyncCounter counter) {
        requests.increment();
        long start = System.nanoTime();
        counter.addAndGet(deltaSupplier.getAsLong()).thenRun(() -> {
            long time = System.nanoTime();
            updateTime(time - start);
            queue.offer(time);
        });
    }

    @Override
    public void run() {
        //updater thread
        try {
            while (!stop.get()) {
                queue.take();
                updateCounter(counter);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
