package org.jgroups.perf;

import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.util.AverageMinMax;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * {@link SyncCounter} benchmark.
 */
public class SyncBenchmark implements CounterBenchmark {

    private BenchmarkRun benchmarkRun;

    @Override
    public void init(int concurrency, ThreadFactory threadFactory, LongSupplier deltaSupplier, AsyncCounter counter) {
        benchmarkRun = new BenchmarkRun(concurrency, counter.sync(), threadFactory, deltaSupplier);
    }

    @Override
    public void start() {
        benchmarkRun.start();
    }

    @Override
    public void stop() {
        benchmarkRun.stop();
    }

    @Override
    public void join() throws InterruptedException {
        benchmarkRun.join();
    }

    @Override
    public long getTotalUpdates() {
        return Arrays.stream(benchmarkRun.updaters)
                .filter(Objects::nonNull)
                .mapToLong(Updater::numUpdates)
                .sum();
    }

    @Override
    public AverageMinMax getResults(boolean printUpdaters, Function<AverageMinMax, String> timePrinter) {
        return Arrays.stream(benchmarkRun.updaters)
                .filter(Objects::nonNull)
                .map(updater -> {
                    if (printUpdaters)
                        System.out.printf("updater %s: updates %s\n", updater.thread.getId(), timePrinter.apply(updater.updateTime));
                    return updater.updateTime;
                }).reduce(new AverageMinMax(), AverageMinMax::merge);

    }

    @Override
    public void close() throws Exception {
        // stop
        stop();
        // interrupt any running threads
        Arrays.stream(benchmarkRun.updaters).map(updater -> updater.thread).forEach(Thread::interrupt);
        benchmarkRun = null;
    }

    private static class BenchmarkRun {
        final CountDownLatch countDownLatch;
        final Updater[] updaters;
        final SyncCounter counter;
        final LongSupplier deltaSupplier;

        BenchmarkRun(int numberOfThreads, SyncCounter counter, ThreadFactory threadFactory, LongSupplier deltaSupplier) {
            this.counter = counter;
            this.deltaSupplier = deltaSupplier;
            countDownLatch = new CountDownLatch(1);
            updaters = new Updater[numberOfThreads];
            for (int i = 0; i < updaters.length; ++i) {
                updaters[i] = new Updater(countDownLatch, counter, deltaSupplier, threadFactory);
                updaters[i].thread.setName("updater-" + i);
                updaters[i].thread.start();
            }
        }

        void start() {
            countDownLatch.countDown();
        }

        void stop() {
            Arrays.stream(updaters).filter(Objects::nonNull).forEach(Updater::stop);
        }

        void join() throws InterruptedException {
            for (Updater updater : updaters) {
                updater.thread.join();
            }
        }
    }

    private static class Updater implements Runnable {
        final CountDownLatch latch;
        final AverageMinMax updateTime = new AverageMinMax(); // in ns
        final SyncCounter counter;
        final LongSupplier deltaSupplier;
        final Thread thread;
        long num_updates;
        volatile boolean running = true;


        public Updater(CountDownLatch latch, SyncCounter counter, LongSupplier deltaSupplier, ThreadFactory threadFactory) {
            this.latch = latch;
            this.counter = counter;
            this.deltaSupplier = deltaSupplier;
            this.thread = threadFactory.newThread(this);
        }

        public long numUpdates() {
            return num_updates;
        }

        public void stop() {
            running = false;
        }

        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            while (running) {
                try {
                    long delta = deltaSupplier.getAsLong();
                    long start = System.nanoTime();
                    counter.addAndGet(delta);
                    long incr_time = System.nanoTime() - start;
                    updateTime.add(incr_time);
                    num_updates++;
                } catch (Throwable t) {
                    if (running)
                        t.printStackTrace();
                }
            }
        }
    }
}
