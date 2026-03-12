package org.jgroups.raft.metrics;

import java.time.Duration;
import java.time.Instant;

/**
 * Metrics about leader election and leadership stability.
 *
 * <p>
 * Provides information about the current leader, when the last election took place, and how long the current leader has
 * been in charge. These metrics are useful for detecting leadership instability; frequent elections suggest network
 * problems or misconfigured failure detectors, and each election causes an unavailability window where the cluster cannot
 * accept operations.
 * </p>
 *
 * <h2>Node-Local Perspective</h2>
 *
 * <p>
 * Election metrics reflect each node's local view. Nodes learn about a new leader at different times depending on when
 * they receive the leader-elected message, so timestamps and durations may vary across the cluster. Only the JGroups
 * coordinator runs the election process; on other nodes, some values may return defaults ({@code null} or {@code -1})
 * until the first election result is received.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public interface ElectionMetrics {

    /**
     * The Raft identifier of the current leader.
     *
     * <p>
     * Use this to route client requests or to verify that a specific node holds leadership. The value updates when this
     * node learns about a new leader, which may happen after the election completes on the coordinator.
     * </p>
     *
     * @return the leader's Raft ID, or a {@code null} string if no leader is known or metrics are disabled.
     */
    String getLeaderRaftId();

    /**
     * The instant at which the last leader election started, as observed by this node.
     *
     * <p>
     * On the JGroups coordinator, this reflects when the voting process began. On other nodes, it returns {@code null},
     * since the non-coordinator nodes do not start the voting process. Compare consecutive values to measure election
     * frequency; elections happening frequently indicate cluster instability.
     * </p>
     *
     * @return the election start time, or {@code null} if no election has occurred yet. Returns
     *         {@link java.time.Instant#EPOCH} when metrics are disabled.
     */
    Instant getLeaderElectionTime();

    /**
     * The time elapsed since the last leader election ended.
     *
     * <p>
     * A steadily growing value indicates a stable leader. If this value stays small or resets frequently, the cluster
     * is experiencing repeated elections, which impacts availability. Use this metric to set up alerts when leadership
     * tenure falls below an expected threshold.
     * </p>
     *
     * @return the duration since the last election ended, or {@link java.time.Duration#ZERO} if no election
     *         has completed or metrics are disabled.
     */
    Duration getTimeSinceLastLeaderChange();
}
