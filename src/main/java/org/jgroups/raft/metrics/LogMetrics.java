package org.jgroups.raft.metrics;

/**
 * Metrics for log replication.
 *
 * // Explain metrics might differ between leader and followers.
 * // Explain in each method how the metrics is calculated.
 *
 * @since 2.0
 * @author José Bolina
 */
public interface LogMetrics {

    long getTotalLogEntries();

    long getReplicatedLogEntries();

    long getUncommittedLogEntries();

    double getReplicationLag();
}
