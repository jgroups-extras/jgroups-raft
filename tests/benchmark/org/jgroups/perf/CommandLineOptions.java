package org.jgroups.perf;

/**
 * Base command line arguments for Raft benchmarks.
 * <p>
 * The first argument <b>must</b> be the FQN of the benchmark class. The remaining arguments can be provided in any order:
 * <ul>
 *     <li><code>-props</code>: The XML file to configure the protocol stack;</li>
 *     <li><code>-name</code>: The raft-id of the current node;</li>
 *     <li><code>-nohup</code>: Disable the event loop;</li>
 *     <li><code>-histogram</code>: Path to write the HdrHistogram file with the collected metrics for this node.</li>
 * </ul>
 * </p>
 */
public final class CommandLineOptions {

    private final String benchmark;
    private final String name;
    private final String props;
    private final String histogramPath;
    private final boolean runEventLoop;

    private CommandLineOptions(String benchmark, String name, String props, String histogramPath, boolean runEventLoop) {
        this.benchmark = benchmark;
        this.name = name;
        this.props = props;
        this.histogramPath = histogramPath;
        this.runEventLoop = runEventLoop;
    }

    public String getBenchmark() {
        return benchmark;
    }

    public String getName() {
        return name;
    }

    public String getProps() {
        return props;
    }

    public String getHistogramPath() {
        return histogramPath;
    }

    public boolean shouldRunEventLoop() {
        return runEventLoop;
    }

    public static CommandLineOptions parse(String[] args) {
        String props = null;
        String name = null;
        String histogramPath = null;
        boolean runEventLoop = true;

        if (args.length == 0)
            throw new IllegalArgumentException("Arguments not provided");

        // The first position contains the benchmark class to run.
        String benchmark = args[0];

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-props":
                    props = args[++i];
                    break;

                case "-name":
                    name = args[++i];
                    break;

                case "-nohup":
                    runEventLoop = false;
                    break;

                case "-histogram":
                    histogramPath = args[++i];
                    break;

                default:
                    System.out.printf("Unknown option: %s%n", args[i]);
                    help(benchmark);
                    break;
            }
        }

        return new CommandLineOptions(benchmark, name, props, histogramPath, runEventLoop);
    }

    private static void help(String benchmark) {
        System.out.printf("%s [-props <props>] [-name <name>] [-nohup] [-histogram /path/to/write]", benchmark);
    }
}
