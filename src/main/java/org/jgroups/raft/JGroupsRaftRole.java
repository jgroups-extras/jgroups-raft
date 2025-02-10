package org.jgroups.raft;

/**
 * Represents the role of a Raft node.
 *
 * <p>
 * Nodes can transition between the following roles:
 * <pre>
 *     NONE ──┬── LEARNER
 *            │
 *            └── FOLLOWER ──── LEADER
 *                    ↑           │
 *                    └───────────┘
 *
 * - NONE: Node not initialized or stopped
 * - LEARNER: Node connects but not in membership list
 * - FOLLOWER: Node connects and is in membership list
 * - LEADER: Elected from FOLLOWER role
 * </pre>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public enum JGroupsRaftRole {

    /**
     * Indicates no active role.
     *
     * <p>
     * This state occurs when the Raft node is not initialized, has been stopped ({@link JGroupsRaft#stop()}), or is in
     * a transitional state before assuming an active role after calling {@link JGroupsRaft#start()}.
     * </p>
     */
    NONE,

    /**
     * The leader role in the Raft consensus algorithm.
     *
     * <p>
     * The leader is responsible for accepting client requests, replicating log entries to followers, and coordinating the
     * consensus process. Only one leader exists per term in a functioning cluster. JGroups Raft handles leader election
     * through {@link org.jgroups.View} changes in JGroups. Therefore, there are no competing candidates or timeouts for
     * starting elections. You can learn more about the implementation in design documents.
     * </p>
     */
    LEADER,

    /**
     * The follower role in the Raft consensus algorithm.
     *
     * <p>
     * Followers passively receive and acknowledge log entries from the leader, participate in leader elections, and can
     * become candidates when the leader becomes unavailable.
     * </p>
     */
    FOLLOWER,

    /**
     * The learner role as described in Raft optimizations.
     *
     * <p>
     * Learners receive log entries from the leader but do not participate in voting or consensus decisions. This role
     * is typically used for nodes that need to catch up on the log or for read-only replicas. Every node that joins the
     * cluster and is not in the Raft membership is considered a learner. This approach allows for dynamic scaling of the
     * cluster without affecting the quorum required for consensus. The node can be promoted to a full member of the Raft
     * cluster by updating the Raft configuration to include it in the membership after it has caught up with all changes.
     * You can learn more about the implementation in design documents.
     * </p>
     */
    LEARNER,
}
