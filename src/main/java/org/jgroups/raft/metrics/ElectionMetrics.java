package org.jgroups.raft.metrics;

import java.time.Duration;
import java.time.Instant;

/**
 * Metrics related to the leader election process.
 *
 * // Explain the values might vary depending on which node it is retrieved. The node might receive the leader elected message at different times.
 *
 * @since 2.0
 * @author José Bolina
 */
public interface ElectionMetrics {

    String getLeaderRaftId();

    Instant getLeaderElectionTime();

    Duration getTimeSinceLastLeaderChange();
}
