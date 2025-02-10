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
     * Triggers the election mechanism.
     *
     * <p>
     *
     * </p>
     * @return
     */
    CompletionStage<String> forceLeaderElection();

    CompletionStage<Void> addNode(String raftId);

    CompletionStage<Void> removeNode(String raftId);

    Set<String> members();
}
