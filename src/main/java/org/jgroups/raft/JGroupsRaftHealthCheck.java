package org.jgroups.raft;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

/**
 * Health check utility for periodically check the node health.
 *
 * // Health check is cluster wide, a single call invokes on all nodes.
 * // The caller can invoke the methods periodically to verify the cluster health.
 *
 * @since 2.0
 * @author José Bolina
 */
public interface JGroupsRaftHealthCheck {

    ClusterHealth getClusterHealth();

    Collection<NodeHealthDetail> getNodeHealthDetails();

    enum ClusterHealth {
        HEALTHY,
        DEGRADED,
        FAILURE,
    }

    record NodeHealthDetail(String raftId, ClusterHealth health, Instant lastCheck, Collection<HealthIssue> issues) {}

    record HealthIssue(String issueId, String description, Severity severity, Instant detectedAt, Map<String, String> extras) {
        enum Severity {
            WARNING,
            ERROR,
            CRITICAL,
        }
    }
}
