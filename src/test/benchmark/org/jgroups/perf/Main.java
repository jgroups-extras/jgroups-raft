package org.jgroups.perf;

import org.jgroups.perf.harness.AbstractRaftBenchmark;
import org.jgroups.util.Util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Entry-point class to run Raft benchmarks.
 * <p>
 * The command line arguments are parsed and the benchmark class is instantiated. The arguments must be provided in the
 * correct order. The very first argument is the benchmark class to run, followed by the arguments.
 * </p>
 */
public class Main {

    public static void main(String[] args) throws Throwable {
        CommandLineOptions cmd = CommandLineOptions.parse(args);
        AbstractRaftBenchmark benchmark = instantiate(cmd);

        // Initializes the benchmark.
        // Causes the nodes to retrieve the benchmark configuration from the coordinator.
        benchmark.init();

        if (cmd.shouldRunEventLoop()) {
            benchmark.eventLoop();
        } else {
            for (;;) Util.sleep(60_000);
        }

        benchmark.stop();
    }

    @SuppressWarnings("unchecked")
    private static AbstractRaftBenchmark instantiate(CommandLineOptions cmd)
            throws InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException {

        Class<? extends AbstractRaftBenchmark> clazz = (Class<? extends AbstractRaftBenchmark>) Class.forName(cmd.getBenchmark());
        Constructor<?>[] constructors = clazz.getConstructors();

        if (constructors.length > 1)
            throw new IllegalStateException("Multiple constructors declared!");

        Constructor<? extends AbstractRaftBenchmark> c = (Constructor<? extends AbstractRaftBenchmark>) constructors[0];
        return c.newInstance(cmd);
    }
}
