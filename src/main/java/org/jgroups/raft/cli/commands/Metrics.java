package org.jgroups.raft.cli.commands;

import org.jgroups.raft.internal.probe.RaftProtocolProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Retrieves critical performance and operational metrics from the Raft cluster.
 *
 * <p>
 * This command provides a deep dive into the internal latency and throughput statistics of the Raft protocol. The output
 * is organized into several categories (displayed as separate tables in the CLI):
 *
 * <ul>
 *   <li><b>General:</b> High-level counts like {@code total-nodes} and {@code active-nodes}.</li>
 *   <li><b>Processing Metrics:</b> Latency stats (p99, p95, avg) for completely processing a user request.</li>
 *   <li><b>Replication Metrics:</b> Latency stats for replicating log entries to followers. From broadcasting the
 *                                   command until committed.</li>
 *   <li><b>Election Metrics:</b> Information about the current leader, last election time, and term duration.</li>
 * </ul>
 * </p>
 *
 * <p>
 * This command is useful for diagnosing performance bottlenecks (e.g., slow disk I/O or network latency) and verifying
 * that the leader is stable.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
@Command(name = "metrics", description = "Display critical performance and operational metrics.")
final class Metrics extends WatchableProbeCommand {

    /**
     * Optional filter to restrict the output to a specific metric category.
     *
     * <p>
     * Use this if you are only interested in a subset of the data (e.g., just "replication-metrics") to reduce visual clutter.
     * </p>
     */
    // TODO: implement CLI filtering.
    @Option(names = "-category", description = "Filter metrics to a specific category.", defaultValue = Option.NULL_VALUE)
    private String category;

    @Override
    protected String probeRequest() {
        return RaftProtocolProbe.PROBE_RAFT_METRICS;
    }
}
