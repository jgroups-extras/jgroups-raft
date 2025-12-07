package org.jgroups.raft;

import org.jgroups.raft.configuration.Property;
import org.jgroups.raft.metrics.ElectionMetrics;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.metrics.LogMetrics;
import org.jgroups.raft.metrics.PerformanceMetrics;

import java.time.Duration;
import java.time.Instant;

import static org.jgroups.raft.configuration.RuntimeProperties.PROPERTY_PREFIX;

/**
 * Entry point for metrics in the raft system.
 *
 * // Explain that metrics are disabled by default.
 *
 * @since 2.0
 * @author José Bolina
 */
public interface JGroupsRaftMetrics {

    String METRICS_PROPERTY_PREFIX = PROPERTY_PREFIX + ".metrics";

    Property METRICS_ENABLED = Property.create(METRICS_PROPERTY_PREFIX + ".enabled")
            .withDisplayName("JGroups Raft metrics enabled")
            .withDescription("Enable metrics collection")
            .withDefaultValue(false)
            .build();

    int getTotalNodes();

    int getActiveNodes();

    ElectionMetrics leaderMetrics();

    LogMetrics replicationMetrics();

    PerformanceMetrics performanceMetrics();

    static JGroupsRaftMetrics disabled() {
        return new DisabledJGroupsRaftMetrics();
    }

    final class DisabledJGroupsRaftMetrics implements JGroupsRaftMetrics {

        private DisabledJGroupsRaftMetrics() { }

        private static final ElectionMetrics ELECTION_METRICS = new ElectionMetrics() {
            @Override
            public String getLeaderRaftId() {
                return "";
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
            public long getReplicatedLogEntries() {
                return -1;
            }

            @Override
            public long getUncommittedLogEntries() {
                return -1;
            }

            @Override
            public double getReplicationLag() {
                return -1;
            }
        };
        private static final LatencyMetrics LATENCY_METRICS = new LatencyMetrics() {
            @Override
            public double getAvgLatency() {
                return -1;
            }

            @Override
            public double getP99Latency() {
                return -1;
            }

            @Override
            public double getP95Latency() {
                return -1;
            }

            @Override
            public double getMaxLatency() {
                return -1;
            }

            @Override
            public double getPercentile(double p) {
                return -1;
            }

            @Override
            public long getTotalMeasurements() {
                return -1;
            }
        };
        private static final PerformanceMetrics PERFORMANCE_METRICS = new PerformanceMetrics() {
            @Override
            public LatencyMetrics getCommandProcessingLatency() {
                return LATENCY_METRICS;
            }

            @Override
            public LatencyMetrics getLeaderElectionLatency() {
                return LATENCY_METRICS;
            }

            @Override
            public LatencyMetrics getReplicationLatency() {
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
    }
}
