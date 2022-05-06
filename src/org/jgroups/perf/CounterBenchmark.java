package org.jgroups.perf;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.util.AverageMinMax;

import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Benchmark implementation used by {@link CounterPerf}.
 * <p>
 * A new instance is created when "start benchmark" command is created.
 * After creation, {@link #init(int, ThreadFactory, LongSupplier, AsyncCounter)} is invoked with the benchmark settings follow by {@link #start()}.
 * The benchmark runs for some time and then {@link #stop()} and {@link #join()} are invoked.
 */
public interface CounterBenchmark extends AutoCloseable {

    /**
     * Initializes with the benchmark settings.
     *
     * @param concurrency   The number of concurrent updaters.
     * @param threadFactory The thread factory (if it needs to create threads).
     * @param deltaSupplier For each "add" operation, the delta from this {@link LongSupplier} must be used.
     * @param counter       The {@link AsyncCounter} to benchmark. Note that the {@link SyncCounter} is available using {@link AsyncCounter#sync()}.
     */
    void init(int concurrency, ThreadFactory threadFactory, LongSupplier deltaSupplier, AsyncCounter counter);

    /**
     * Signals the test start.
     */
    void start();

    /**
     * Signals the test end.
     */
    void stop();

    /**
     * Wait until all updaters finish their work.
     *
     * @throws InterruptedException If interrupted.
     */
    void join() throws InterruptedException;

    /**
     * @return The total number of "add" operation invoked.
     */
    long getTotalUpdates();

    /**
     * Returns the results of the run.
     *
     * @param printUpdaters If supported and if {@code true}, print to {@link System#out} each updater result.
     * @param timePrinter   {@link Function} to use to print each updater {@link AbstractHistogram} result.
     * @return The {@link Histogram} with the results of all updaters.
     */
    Histogram getResults(boolean printUpdaters, Function<AbstractHistogram, String> timePrinter);

}
