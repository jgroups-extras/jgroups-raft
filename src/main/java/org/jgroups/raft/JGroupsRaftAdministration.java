package org.jgroups.raft;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import net.jcip.annotations.ThreadSafe;

/**
 * Administrative operations for the Raft cluster.
 *
 * <p>
 * This interface provides operations to manage the Raft cluster, including:
 *
 * <ul>
 *     <li>Trigger leader elections.</li>
 *     <li>Add or remove Raft members.</li>
 *     <li>Create snapshots of the state machine.</li>
 * </ul>
 *
 * These operations are intended to be utilized during maintenance by operators. We recommend using them sparingly.
 * </p>
 *
 * <h2>Leader Election</h2>
 *
 * <p>
 * The Raft protocol utilizes a leader to replicate the commands in the cluster. Our implementation differs from the one described
 * in the Raft paper. We leverage JGroups' capabilities to ensure more stable leadership. Elections are based on {@link org.jgroups.View}
 * updates from nodes leaving and joining the cluster. For more information, see the design documents.
 * </p>
 *
 * <p>
 * This interface provides an API to trigger leader election without requiring a {@link org.jgroups.View} update. However,
 * note that the election procedure still adheres to the safety requirements of the Raft protocol to select a new leader.
 * Consequently, it is possible for the previous leader to be elected again.
 * </p>
 *
 * <h2>Dynamic Membership</h2>
 *
 * <p>
 * The Raft protocol allows dynamic membership changes. Nodes can be added or removed from the cluster without disruptions.
 * We implement a mechanism that performs a single membership change at a time. For example, adding two new nodes requires
 * submitting two operations.
 * </p>
 *
 * <p>
 * The same approach applies to adding or removing nodes. Membership operations must be submitted by the leader and require
 * a quorum of nodes to agree on the configuration change. This means that membership operations are not possible if the cluster
 * is unhealthy; extra care is needed to avoid disrupting the cluster. Consider the following points:
 *
 * <ul>
 *     <li>Fresh and stateless nodes can cause an availability gap in the system.
 *         <p>
 *             Nodes added to the cluster without any state will need to catch up with the leader. If the quorum size changes
 *             with the new node, the cluster will not be able to make progress until the new node is fully caught up.
 *         </p>
 *     </li>
 *
 *     <li>A leader removing itself from the cluster.
 *         <p>
 *             In this case, the leader replicates an operation to remove itself from the cluster. The leader must replicate
 *             the operation but should not count itself in the quorum. After the operation is completed, the leader must step down.
 *         </p>
 *     </li>
 *
 *     <li>A node not in the member list.
 *         <p>
 *             A node can be removed from the Raft member list but still remain connected to the JGroups cluster. This node will
 *             transition to the role of learner and won't count towards quorum.
 *         </p>
 *     </li>
 * </ul>
 *
 * Although the Raft protocol guarantees strong consistency and safety, it is important to understand how administrative operations
 * can affect the cluster. These operations should be used with care for maintenance.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see org.jgroups.protocols.raft.DynamicMembership
 * @see org.jgroups.protocols.raft.ELECTION
 * @see org.jgroups.protocols.raft.ELECTION2
 */
@ThreadSafe
public interface JGroupsRaftAdministration {

    /**
     * Forces a new leader election, excluding the current leader from candidacy.
     *
     * <p>
     * This operation triggers the election mechanism without requiring a {@link org.jgroups.View} update. The current
     * leader is excluded from candidacy so that a different node is elected. If no leader exists at the time of the call,
     * the election proceeds without exclusion.
     * </p>
     *
     * <p>
     * This method must be called on the JGroups view coordinator. Calling it on a non-coordinator node completes the
     * returned stage exceptionally with {@link IllegalStateException}. Use this operation sparingly, for example, to
     * offload leadership before removing a node from the cluster.
     * </p>
     *
     * <p>
     * The election still adheres to the Raft safety requirements: only a node with a sufficiently up-to-date log can
     * be elected. If the excluded leader is the only node with the highest log, the election retries until another node
     * catches. If the cluster is running below majority, the returned stage completes exceptionally. The future also
     * completes exceptionally if the election mechanism is stopped externally (e.g., a view change stops the procedure).
     * </p>
     *
     * <p>
     * Callers can set a timeout on the returned stage. Otherwise, the election mechanism will run indefinitely.
     * Cancelling the stage stops the election mechanism:
     * </p>
     *
     * <pre>{@code
     * administration.forceLeaderElection()
     *     .toCompletableFuture()
     *     .orTimeout(10, TimeUnit.SECONDS)
     *     .whenComplete((leader, err) -> {
     *         if (err != null) {
     *             // Election timed out or failed.
     *         }
     *     });
     * }</pre>
     *
     * @return a stage that completes with the Raft ID of the newly elected leader.
     */
    CompletionStage<String> forceLeaderElection();

    /**
     * Adds a new voting member to the Raft cluster.
     *
     * <p>
     * Submits a membership change that adds the given Raft ID as a voting member. The operation is replicated through the
     * Raft log and requires a quorum of the current members to commit. The returned stage completes when the membership
     * change is committed.
     * </p>
     *
     * <p>
     * This operation must be submitted to the leader. If a {@link org.jgroups.protocols.raft.REDIRECT} protocol is present
     * in the stack, the request is forwarded automatically; otherwise, calling this on a cluster without leader completes the
     * stage exceptionally.
     * </p>
     *
     * <p>
     * Only one membership change can be in progress at a time. Adding a node that is already a member completes the stage
     * exceptionally. Be aware that adding a fresh, stateless node might increase the quorum size; the cluster may stall
     * until the new node catches up with the leader's log. Consider adding the node first as learner, just joining the cluster,
     * and then promoting it to a voting member.
     * </p>
     *
     * @param raftId the Raft identifier of the node to add. Must match the {@code raft_id} configured on the target node.
     * @return a stage that completes when the membership change is committed.
     */
    CompletionStage<Void> addNode(String raftId);

    /**
     * Removes a voting member from the Raft cluster.
     *
     * <p>
     * Submits a membership change that removes the given Raft ID from the set of voting members. The operation is replicated
     * through the Raft log and requires a quorum of the current members to commit. The returned stage completes when the
     * membership change is committed.
     * </p>
     *
     * <p>
     * This operation must be submitted to the leader. If a {@link org.jgroups.protocols.raft.REDIRECT} protocol is present
     * in the stack, the request is forwarded automatically; otherwise, calling this on a cluster without a leader node
     * completes the stage exceptionally.
     * </p>
     *
     * @param raftId the Raft identifier of the node to remove.
     * @return a stage that completes when the membership change is committed.
     */
    CompletionStage<Void> removeNode(String raftId);

    /**
     * Returns the current set of voting members in the Raft cluster.
     *
     * <p>
     * The returned set reflects the committed membership configuration as known by this node. During a pending membership
     * change, the set may not yet include a recently added node or may still include a recently removed one until the change
     * is committed.
     * </p>
     *
     * @return an unmodifiable set of Raft identifiers of the current voting members.
     */
    Set<String> members();

    /**
     * Triggers a snapshot of the current state machine and truncates the log.
     *
     * <p>
     * A snapshot captures the current state machine state and allows the log to be truncated, reclaiming storage. Snapshots
     * are also used to bring slow or new followers up to date without replaying the entire log. Taking a snapshot means
     * stopping all reads and writes operations happening to the state machine until the procedure completes.
     * </p>
     *
     * @return a stage that completes when the snapshot is done.
     */
    CompletionStage<Void> snapshot();
}
