package org.jgroups.raft.cli.probe;

/**
 * Defines the contract for executing JGroups Probe requests.
 *
 * <p>
 * This interface abstracts the underlying mechanism used to send requests to the cluster. It primarily exists to decouple
 * the CLI commands from the concrete network implementation, allowing for easier unit testing by mocking the runner.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see JGroupsProbeRunner
 */
public interface ProbeRunner {

    static ProbeRunner defaultProbeRunner() {
        return JGroupsProbeRunner.INSTANCE;
    }

    /**
     * Executes a probe request based on the provided arguments.
     *
     * @param arguments The configuration for the probe request (target addresses, query, timeout, etc.).
     * @param handler   The callback handler to process and display the responses received.
     * @throws Throwable If any error occurs during the network transmission or response processing.
     */
    void run(ProbeArguments arguments, ProbeResponseWriter handler) throws Throwable;
}
