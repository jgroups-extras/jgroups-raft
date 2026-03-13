package org.jgroups.raft;

import static org.jgroups.raft.configuration.RuntimeProperties.PROPERTY_PREFIX;

import org.jgroups.raft.configuration.Property;
import org.jgroups.raft.metrics.ElectionMetrics;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.metrics.LogMetrics;
import org.jgroups.raft.metrics.PerformanceMetrics;

import java.time.Duration;
import java.time.Instant;

/**
 * Monitoring interface for a Raft node.
 *
 * <p>
 * Provides access to cluster membership, leader election information, log replication status, and operation latency
 * measurements. Each metric category is exposed through a dedicated sub-interface: {@link ElectionMetrics}, {@link LogMetrics},
 * and {@link PerformanceMetrics}.
 * </p>
 *
 * <h2>Node-Local Metrics</h2>
 *
 * <p>
 * All metrics are local to the node they are retrieved from. Different nodes may report different values for the same metric
 * depending on their role and when they received cluster-wide messages. For example, a follower's election timestamp reflects
 * when it learned about the new leader, not when the election completed on the coordinator. To get a complete picture
 * of the cluster, collect metrics from all nodes.
 * </p>
 *
 * <h2>Enabling Metrics</h2>
 *
 * <p>
 * Metrics collection is disabled by default. Enable it by setting the {@code jgroups.raft.metrics.enabled} runtime property:
 * </p>
 *
 * <pre>{@code
 * JGroupsRaft.builder(stateMachine, Api.class)
 *     .withRuntimeProperties(RuntimeProperties.from(Map.of("jgroups.raft.metrics.enabled", "true")))
 *     .build();
 * }</pre>
 *
 * <p>
 * When disabled, all methods return safe default values: {@code -1} for numeric metrics, empty strings for identifiers,
 * and {@link java.time.Instant#EPOCH} or {@link java.time.Duration#ZERO} for time values. A disabled instance can be
 * obtained via {@link JGroupsRaftMetrics#disabled()}.
 * </p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 * Implementations are thread-safe. Metrics can be read concurrently from any thread without external synchronization.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see ElectionMetrics
 * @see LogMetrics
 * @see PerformanceMetrics
 */
public interface JGroupsRaftMetrics {

    String METRICS_PROPERTY_PREFIX = PROPERTY_PREFIX + ".metrics";

    Property METRICS_ENABLED = Property.create(METRICS_PROPERTY_PREFIX + ".enabled")
            .withDisplayName("JGroups Raft metrics enabled")
            .withDescription("Enable metrics collection")
            .withDefaultValue(false)
            .build();

    /**
     * The total number of nodes configured in the cluster, excluding learners.
     *
     * <p>
     * This value reflects the static Raft membership. The member in the list include only the voting members of the
     * algorithm. This list reflects the currently configured members, and as such, it only changes through membership
     * operation submitted through {@link JGroupsRaftAdministration}. The {@link #getActiveNodes()} returns a live
     * view of the cluster members.
     * </p>
     *
     * @return the configured cluster size, or {@code -1} if metrics are disabled.
     */
    int getTotalNodes();

    /**
     * The number of nodes currently reachable in the cluster view.
     *
     * <p>
     * Compare this value against {@link #getTotalNodes()} to detect membership degradation. When active nodes drop below
     * the majority threshold ({@code totalNodes / 2 + 1}), the cluster becomes unavailable until enough members rejoin.
     * Monitoring this metric is essential for alerting on availability risks before the cluster loses quorum.
     * </p>
     *
     * @return the number of active nodes (including learners), or {@code -1} if metrics are disabled.
     */
    int getActiveNodes();

    /**
     * Metrics about the current leader and election timing.
     *
     * <p>
     * Use these metrics to identify the current leader, determine when the last election occurred, and detect leadership
     * instability. Frequent leader changes may indicate network issues or misconfigured failure detectors, and each election
     * causes a brief period where operations are unavailable.
     * </p>
     *
     * @return the leader election metrics; never {@code null}.
     * @see ElectionMetrics
     */
    ElectionMetrics leaderMetrics();

    /**
     * Metrics about the Raft log, including entry counts and replication progress.
     *
     * <p>
     * Use these metrics to monitor replication health. A growing gap between total and replicated entries indicates that
     * followers are falling behind.
     * </p>
     *
     * @return the log replication metrics; never {@code null}.
     * @see LogMetrics
     */
    LogMetrics replicationMetrics();

    /**
     * Latency measurements for the main operations: request processing, consensus, leader election, and request forwarding.
     *
     * <p>
     * Use these metrics to identify performance bottlenecks and validate SLAs. Comparing the different latency categories
     * helps pinpoint whether delays originate from consensus, request queuing, or network forwarding. See {@link PerformanceMetrics}
     * for details on each category.
     * </p>
     *
     * @return the performance metrics; never {@code null}.
     * @see PerformanceMetrics
     */
    PerformanceMetrics performanceMetrics();

    /**
     * Resets all collected metrics to their initial state.
     *
     * <p>
     * Clears latency histograms, counters, and any accumulated statistics across all metric categories (election, log,
     * performance). Membership metrics ({@link #getTotalNodes()}, {@link #getActiveNodes()}) are not affected since they
     * reflect live cluster state rather than accumulated measurements.
     * </p>
     *
     * <p>
     * This is useful for establishing a clean baseline after configuration changes, deployments, or when investigating
     * a specific time window. Has no effect when metrics are disabled.
     * </p>
     */
    void reset();

    static JGroupsRaftMetrics disabled() {
        return new DisabledJGroupsRaftMetrics();
    }

    final class DisabledJGroupsRaftMetrics implements JGroupsRaftMetrics {

        private DisabledJGroupsRaftMetrics() { }

        private static final ElectionMetrics ELECTION_METRICS = new ElectionMetrics() {
            @Override
            public String getLeaderRaftId() {
                return null;
            }

            @Override
            public Instant getLeaderElectionTime() {
                return Instant.EPOCH;
            }

            @Override
            public Duration getTimeSinceLastLeaderChange() {
                return Duration.ZERO;
            }
        };
        private static final LogMetrics LOG_METRICS = new LogMetrics() {
            @Override
            public long getTotalLogEntries() {
                return -1;
            }

            @Override
            public long getCommittedLogEntries() {
                return -1;
            }

            @Override
            public long getUncommittedLogEntries() {
                return -1;
            }

            @Override
            public long getLogSizeInBytes() {
                return -1;
            }

            @Override
            public long getCurrentTerm() {
                return -1;
            }

            @Override
            public long getCommitIndex() {
                return -1;
            }

            @Override
            public int getSnapshotCount() {
                return -1;
            }

            @Override
            public int getSnapshotsReceived() {
                return -1;
            }
        };
        private static final LatencyMetrics LATENCY_METRICS = LatencyMetrics.disabled();
        private static final PerformanceMetrics PERFORMANCE_METRICS = new PerformanceMetrics() {

            @Override
            public LatencyMetrics getTotalLatency() {
                return LATENCY_METRICS;
            }

            @Override
            public LatencyMetrics getProcessingLatency() {
                return LATENCY_METRICS;
            }

            @Override
            public LatencyMetrics getLeaderElectionLatency() {
                return LATENCY_METRICS;
            }

            @Override
            public LatencyMetrics getRedirectLatency() {
                return LATENCY_METRICS;
            }
        };

        @Override
        public int getTotalNodes() {
            return -1;
        }

        @Override
        public int getActiveNodes() {
            return -1;
        }

        @Override
        public ElectionMetrics leaderMetrics() {
            return ELECTION_METRICS;
        }

        @Override
        public LogMetrics replicationMetrics() {
            return LOG_METRICS;
        }

        @Override
        public PerformanceMetrics performanceMetrics() {
            return PERFORMANCE_METRICS;
        }

        @Override
        public void reset() { }
    }
}
