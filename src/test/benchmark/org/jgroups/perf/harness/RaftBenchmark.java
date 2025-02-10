package org.jgroups.perf.harness;

import java.util.function.Function;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;

public interface RaftBenchmark extends AutoCloseable {
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
