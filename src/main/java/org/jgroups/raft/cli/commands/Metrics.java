package org.jgroups.raft.cli.commands;

import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.cli.probe.writer.CategoryFilterResponseWriter;
import org.jgroups.raft.internal.probe.MetricCategory;
import org.jgroups.raft.internal.probe.RaftProtocolProbe;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

import picocli.CommandLine;
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
 *   <li><b>Election Metrics:</b> Information about the current leader, last election time, and term duration.</li>
 *   <li><b>Total Latency:</b> End-to-end latency stats (p99, p95, avg) for processing a user request.</li>
 *   <li><b>Processing Latency:</b> Latency stats for local processing (excludes replication wait).</li>
 *   <li><b>Election Latency:</b> Latency stats for completing leader election rounds.</li>
 *   <li><b>Redirect Latency:</b> Latency stats for follower-to-leader request forwarding.</li>
 *   <li><b>Log Metrics:</b> Log entry counts, log size, commit index, and snapshot statistics.</li>
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
    @Option(
            names = "-category",
            description = "Filter metrics to a specific category.",
            converter = MetricCategoryConverter.class,
            completionCandidates = MetricCategoryCandidate.class,
            defaultValue = Option.NULL_VALUE)
    private MetricCategory category;

    @Override
    protected String probeRequest() {
        return RaftProtocolProbe.PROBE_RAFT_METRICS;
    }

    @Override
    public void handler(ProbeResponseWriter handler) {
        if (category != null) {
            super.handler(new CategoryFilterResponseWriter(handler, category));
        } else {
            super.handler(handler);
        }
    }

    /**
     * Converts kebab-case category tokens (e.g., "processing-latency") to {@link MetricCategory} enum values.
     */
    private static final class MetricCategoryConverter implements CommandLine.ITypeConverter<MetricCategory> {
        @Override
        public MetricCategory convert(String value) {
            if (value == null)
                return null;

            for (MetricCategory category : MetricCategory.values()) {
                if (Objects.equals(category.key(), value))
                    return category;
            }

            throw new IllegalArgumentException(
                    String.format("Invalid category '%s', Valid categories: %s",
                            value, Arrays.toString(MetricCategory.values()))
            );
        }
    }

    /**
     * Provides completion candidates for the {@code -category} option.
     */
    private static final class MetricCategoryCandidate implements Iterable<String> {
        private static final String[] CANDIDATES = Arrays.stream(MetricCategory.values())
                .map(MetricCategory::key)
                .toArray(String[]::new);

        @Override
        public Iterator<String> iterator() {
            return Arrays.asList(CANDIDATES).iterator();
        }
    }
}
