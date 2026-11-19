package org.jgroups.raft;

/**
 * Read-only view of the current Raft protocol state.
 * <p>
 * This interface provides access to key Raft state variables for monitoring
 * and observability purposes. All values reflect the local node's view of
 * the cluster state at the time of access.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see <a href="https://raft.github.io/raft.pdf">The Raft Consensus Algorithm</a>
 */
public interface JGroupsRaftState {

    /**
     * Returns the identifier of the local node.
     *
     * @return the local node identifier
     */
    String id();

    /**
     * Returns the identifier of the current leader node.
     *
     * <p>
     * May return {@code null} if no leader is currently known or during leader election periods.
     * </p>
     *
     * @return the leader node identifier, or {@code null} if unknown
     */
    String leader();

    /**
     * Returns the current term number.
     *
     * <p>
     * Terms are used to detect obsolete information and ensure logical time ordering in the Raft algorithm.
     * </p>
     *
     * @return the current term
     */
    long term();

    /**
     * Returns the index of the highest log entry known to be committed.
     *
     * <p>
     * Committed entries are guaranteed to be durable and will not be lost.
     * </p>
     *
     * @return the commit index
     */
    long commitIndex();

    /**
     * Returns the index of the highest log entry applied to the state machine.
     *
     * <p>
     * Entries are applied in order as soon as they are committed, so this value is equal to the commit index. If applying
     * an entry fails, the value stops before that entry.
     * </p>
     *
     * @return the last applied index
     */
    long lastApplied();
}
