package org.jgroups.perf.replication;

import org.jgroups.perf.harness.RaftBenchmark;
import org.jgroups.perf.counter.HistogramUtil;
import org.jgroups.raft.Settable;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;

/**
 * Benchmark for synchronous APIs.
 * <p>
 * Initialize multiple threads simultaneously and execute the synchronous API of {@link org.jgroups.raft.RaftHandle}
 * for replicating the payload.
 * </p>
 *
 * @author José Bolina
 */
class SyncReplicationBenchmark implements RaftBenchmark {

    private final BenchmarkRunner runner;

    public SyncReplicationBenchmark(int numberThreads, Settable settable, byte[] payload, ThreadFactory tf) {
        this.runner = new BenchmarkRunner(numberThreads, settable, payload, tf);
    }

    @Override
    public void start() {
        this.runner.start();
    }

    @Override
    public void stop() {
        this.runner.stop();
    }

    @Override
    public void join() throws InterruptedException {
        this.runner.join();
    }

    @Override
    public long getTotalUpdates() {
        return Arrays.stream(runner.updaters)
                .filter(Objects::nonNull)
                .mapToLong(BenchmarkUpdater::numUpdates)
                .sum();
    }

    @Override
    public Histogram getResults(boolean printUpdaters, Function<AbstractHistogram, String> timePrinter) {
        Histogram global = HistogramUtil.create();
        Arrays.stream(runner.updaters)
                .filter(Objects::nonNull)
                .map(updater -> {
                    if (printUpdaters)
                        System.out.printf("updater %s: updates %s%n", updater.thread.getId(), timePrinter.apply(updater.histogram));
                    return updater.histogram;
                })
                .forEach(global::add);
        return global;
    }

    @Override
    public void close() throws Exception {
        stop();
        Arrays.stream(runner.updaters)
                .map(updater -> updater.thread)
                .forEach(Thread::interrupt);
    }

    private static class BenchmarkRunner {
        private final CountDownLatch latch;
        private final BenchmarkUpdater[] updaters;

        public BenchmarkRunner(int numberThreads, Settable settable, byte[] payload, ThreadFactory tf) {
            this.latch = new CountDownLatch(1);
            this.updaters = new BenchmarkUpdater[numberThreads];
            for (int i = 0; i < numberThreads; i++) {
                updaters[i] = new BenchmarkUpdater(latch, settable, payload, tf);
                updaters[i].thread.setName("updater-" + i);
                updaters[i].thread.start();
            }
        }

        public void start() {
            latch.countDown();
        }

        public void stop() {
            for (BenchmarkUpdater updater : updaters) {
                updater.stop();
            }
        }

        public void join() throws InterruptedException {
            for (BenchmarkUpdater updater : updaters) {
                updater.thread.join();
            }
        }
    }

    private static final class BenchmarkUpdater implements Runnable {
        private final Histogram histogram = HistogramUtil.create();
        private final CountDownLatch latch;
        private final Settable settable;
        private final byte[] payload;
        private final Thread thread;

        private long updates;

        private volatile boolean running = true;

        public BenchmarkUpdater(CountDownLatch latch, Settable settable, byte[] payload, ThreadFactory tf) {
            this.latch = latch;
            this.settable = settable;
            this.payload = payload;
            this.thread = tf.newThread(this);
        }

        public long numUpdates() {
            return updates;
        }

        public void stop() {
            running = false;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
                System.out.println("Benchmark updater interrupted before starting");
                return;
            }

            while (running) {
                try {
                    long start = System.nanoTime();
                    settable.set(payload, 0, payload.length);
                    long duration = System.nanoTime() - start;
                    histogram.recordValue(duration);
                    updates++;
                } catch (Exception e) {
                    if (running)
                        e.printStackTrace(System.err);
                }
            }
        }
    }
}
