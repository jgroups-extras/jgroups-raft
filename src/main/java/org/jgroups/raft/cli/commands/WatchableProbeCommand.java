package org.jgroups.raft.cli.commands;

import org.jgroups.raft.cli.probe.writer.RepeatableResponseWriter;
import org.jgroups.util.Util;

import java.util.concurrent.TimeUnit;

import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Abstract base class for commands that support continuous "watch" mode execution.
 *
 * <p>
 * This class extends the standard probe capabilities by adding the ability to run the command repeatedly at a fixed interval,
 * similar to the Unix {@code watch} utility. It is useful for monitoring dynamic cluster state (e.g., metric updates) in real-time.
 * </p>
 *
 * <p>
 * When watch mode is enabled, this class automatically wraps the output handler with a {@link RepeatableResponseWriter}
 * to manage screen clearing or separators between iterations.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
abstract class WatchableProbeCommand extends BaseProbeCommand {

    @Option(names = {"-w", "-watch"}, description = "Run the command continuously in a given interval", defaultValue = "false")
    private boolean watch;

    @Option(names = {"-i", "--interval"}, description = "Interval in seconds to repeat the command. Only takes effect with watch flag. (${DEFAULT-VALUE} s)", defaultValue = "5")
    private int watchIntervalSeconds;

    /**
     * Executes the command logic, optionally entering a loop if watch mode is enabled.
     *
     * <p>
     * If {@code -w} is not provided, this simply delegates to {@link BaseProbeCommand#execute()}. However, if {@code -w}
     * is enabled:
     *
     * <ol>
     *   <li>Validates that the watch interval is not shorter than the network timeout (printing a warning if so).</li>
     *   <li>Wraps the response handler to support multi-execution output (e.g., clearing the screen).</li>
     *   <li>Enters an infinite loop: running the probe, then sleeping for the calculated remainder of the interval.</li>
     * </ol>
     */
    @Override
    protected final void execute() {
        if (!watch) {
            super.execute();
            return;
        }

        long i = watchIntervalSeconds * 1_000L;
        // Warn the user if they try to poll faster than the network timeout allows
        if (i < timeout()) {
            out().println(Ansi.AUTO.string(String.format("@|yellow Watch interval (%s) should be greater than operation timeout (%s).|@", Util.printTime(i, TimeUnit.MILLISECONDS), Util.printTime(timeout(), TimeUnit.MILLISECONDS))));
        }

        // Decorate the existing handler to handle repetitive output (clear screen/separators)
        handler(new RepeatableResponseWriter(handler()));

        // Calculate sleep time:
        // we subtract the timeout to try and maintain a consistent cadence, but ensure we never sleep for a negative amount.
        long interval = Math.max(0, i - timeout());
        while (true) {
            super.execute();
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
