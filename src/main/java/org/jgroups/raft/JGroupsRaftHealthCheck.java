package org.jgroups.raft;

import net.jcip.annotations.ThreadSafe;

/**
 * Health monitoring for a Raft node.
 *
 * <p>
 * Provides liveness, readiness, and cluster-level health information from the local node's perspective. All methods are
 * node-local; they inspect local state only and never issue remote calls, making them safe for frequent polling without
 * impacting cluster performance.
 * </p>
 *
 * <h2>Liveness and Readiness Probes</h2>
 *
 * <p>
 * The two boolean methods map directly to container orchestration health checks:
 * </p>
 *
 * <ul>
 *     <li>{@link #isNodeLive()}: the process is functional. When {@code false}, the node should be restarted.
 *         Suitable for Kubernetes liveness probes.</li>
 *     <li>{@link #isNodeReady()}: the node can serve requests. When {@code false}, traffic should be routed away.
 *         Suitable for Kubernetes readiness probes.</li>
 * </ul>
 *
 * <h2>Cluster Health</h2>
 *
 * <p>
 * {@link #getClusterHealth()} returns a coarse-grained view of the cluster. The status reflects the local node's knowledge
 * of the cluster membership and leader state; different nodes may briefly disagree during view changes or elections.
 * For a complete picture, collect health from all nodes.
 * </p>
 *
 * <h2>Availability Before Start</h2>
 *
 * <p>
 * This interface is available from the moment {@link JGroupsRaft} is constructed, even before {@link JGroupsRaft#start()}
 * is called. Before start, {@link #isNodeLive()} and {@link #isNodeReady()} return {@code false}, and
 * {@link #getClusterHealth()} returns {@link ClusterHealth#NOT_RUNNING}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaft#healthCheck()
 */
@ThreadSafe
public interface JGroupsRaftHealthCheck {

    /**
     * Whether the node process is functional.
     *
     * <p>
     * Returns {@code true} when the JGroups channel is connected and not closed. Use this for <b>liveness probes</b>
     * (e.g., Kubernetes). A {@code false} value means the process is not started or should be restarted.
     * </p>
     *
     * @return {@code true} if the node is live.
     */
    boolean isNodeLive();

    /**
     * Whether the node can serve requests.
     *
     * <p>
     * Returns {@code true} when the channel is connected, a leader is known, and the instance is started. Use this for
     * <b>readiness probes</b> (e.g., Kubernetes). A {@code false} value means traffic should be routed away from this node.
     * </p>
     *
     * @return {@code true} if the node is ready to handle requests.
     */
    boolean isNodeReady();

    /**
     * Overall cluster health from this node's perspective.
     *
     * <ul>
     *     <li>{@code NOT_RUNNING}: channel is disconnected.</li>
     *     <li>{@code HEALTHY}: leader exists and all configured members are active.</li>
     *     <li>{@code DEGRADED}: leader exists and active nodes >= majority but not all.</li>
     *     <li>{@code FAILURE}: no leader or active nodes below majority.</li>
     * </ul>
     *
     * @return the cluster health status.
     */
    ClusterHealth getClusterHealth();

    /**
     * Coarse-grained cluster health status from a single node's perspective.
     *
     * <p>
     * The status is derived from local knowledge of the JGroups view and Raft leader state.
     * Only voting members count towards the health assessment -- learners are excluded.
     * </p>
     */
    enum ClusterHealth {

        /**
         * The channel is disconnected or the node has not started.
         */
        NOT_RUNNING,

        /**
         * A leader exists and all configured voting members are active.
         */
        HEALTHY,

        /**
         * A leader exists and a majority of voting members are active, but some are missing.
         */
        DEGRADED,

        /**
         * No leader is elected, or active voting members are below the majority threshold.
         */
        FAILURE,
    }
}
