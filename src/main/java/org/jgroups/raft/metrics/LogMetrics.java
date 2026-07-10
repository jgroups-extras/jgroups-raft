package org.jgroups.raft.metrics;

import java.time.Duration;

import net.jcip.annotations.ThreadSafe;

/**
 * Metrics about the Raft log state on this node.
 *
 * <p>
 * Provides visibility into how many entries the log contains, how many have been committed, and how much storage the log
 * consumes. These metrics are essential for monitoring replication health and deciding when maintenance operations like
 * snapshots are needed.
 * </p>
 *
 * <h2>Node-Local Perspective</h2>
 *
 * <p>
 * All values reflect this node's local log state. Followers may temporarily report fewer committed entries than the leader
 * if replication is still in progress. To get a cluster-wide picture, collect metrics from all nodes and compare their
 * committed entry counts and terms.
 * </p>
 *
 * <h2>Log Lifecycle</h2>
 *
 * <p>
 * Entries are first appended to the log, then committed once a majority of nodes have acknowledged them. Over time,
 * snapshots compact the log by removing old entries, which reduces {@link #getTotalLogEntries()} and
 * {@link #getLogSizeInBytes()} without affecting the commit index. A growing gap between total and committed entries on
 * a follower indicates it is falling behind the leader.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see PerformanceMetrics
 */
@ThreadSafe
public interface LogMetrics {

    /**
     * The total number of entries currently in the log.
     *
     * <p>
     * This count reflects entries remaining after any snapshot truncation. A steadily growing value without periodic
     * decreases suggests snapshots are not being triggered, which may lead to excessive disk usage. Compare against
     * {@link #getCommittedLogEntries()} to see how many entries are still pending a commit.
     * </p>
     *
     * @return the number of log entries, or {@code -1} if metrics are disabled.
     */
    long getTotalLogEntries();

    /**
     * The number of entries committed on this node.
     *
     * <p>
     * An entry is committed once the leader confirms that a majority of nodes have appended it. On the leader, this value
     * advances as soon as quorum is reached. On followers, it advances when the leader notifies them of the new commit
     * index. A follower whose committed count is significantly lower than the leader's may be experiencing network delays
     * or slow disk writes.
     * </p>
     *
     * @return the number of committed entries, or {@code -1} if metrics are disabled.
     */
    long getCommittedLogEntries();

    /**
     * The number of entries appended but not yet committed on this node.
     *
     * <p>
     * On the leader, uncommitted entries are waiting for majority acknowledgment. On followers, they are entries received
     * from the leader but not yet confirmed as committed. A value that stays high or grows over time indicates the cluster
     * is struggling to reach consensus, possibly due to unreachable members or network issues.
     * </p>
     *
     * @return the number of uncommitted entries, or {@code -1} if metrics are disabled.
     */
    long getUncommittedLogEntries();

    /**
     * The physical storage size of the log, in bytes.
     *
     * <p>
     * Tracks how much disk space the log occupies. Use this metric alongside {@link #getSnapshotCount()} to verify that
     * snapshot compaction is keeping storage usage under control. A value that grows unbounded suggests the snapshot
     * threshold may need adjustment.
     * </p>
     *
     * @return the log size in bytes, or {@code -1} if metrics are disabled.
     */
    long getLogSizeInBytes();

    /**
     * The current Raft term as observed by this node.
     *
     * <p>
     * The term increments with each new election. All nodes in a healthy cluster should converge to the same term. A node
     * reporting a lower term than others may have been partitioned and is catching up. A rapidly increasing term across
     * the cluster indicates frequent elections.
     * </p>
     *
     * @return the current term, or {@code -1} if metrics are disabled.
     */
    long getCurrentTerm();

    /**
     * The current commit index on this node.
     *
     * <p>
     * The commit index is the highest log index known to be committed. On a healthy cluster, all nodes should eventually
     * converge to the same commit index. Compare this value across nodes to detect replication delays; a follower lagging
     * behind the leader's commit index is still catching up.
     * </p>
     *
     * @return the commit index, or {@code -1} if metrics are disabled.
     */
    long getCommitIndex();

    /**
     * The number of snapshots this node has performed.
     *
     * <p>
     * Snapshots compact the log by persisting the current state machine state and removing old entries. A zero value on
     * a long-running node may explain high storage usage. Monitor this alongside {@link #getLogSizeInBytes()} to verify
     * that compaction is occurring as expected. A proper tuning is needed for taking snapshots, there is a trade-off
     * between performance vs. disk usage. Taking a snapshot freezes all operations to the state machine until the snapshot
     * finishes.
     * </p>
     *
     * @return the snapshot count, or {@code -1} if metrics are disabled.
     */
    int getSnapshotCount();

    /**
     * The number of snapshot-install messages received from the leader.
     *
     * <p>
     * When a follower falls too far behind, the leader sends its snapshot instead of individual log entries. A high value
     * indicates this node has frequently been unable to keep up with normal replication, which is more expensive than
     * regular log replication for both the leader and the follower.
     * </p>
     *
     * @return the number of snapshots received, or {@code -1} if metrics are disabled.
     */
    int getSnapshotsReceived();

    /**
     * The number of times snapshot creation failed on this node.
     *
     * <p>
     * A snapshot creation failure means the node attempted to take a snapshot but the operation did not complete
     * successfully. The snapshot is retried automatically when the log size threshold is reached again. Persistent
     * failures may indicate a problem in the state machine's snapshot implementation.
     * </p>
     *
     * @return the number of failed snapshot creations, or {@code -1} if metrics are disabled.
     */
    int getFailedSnapshotCreations();

    /**
     * The number of times snapshot installation failed on this node.
     *
     * <p>
     * A snapshot installation failure means the node received a snapshot from the leader but could not apply it.
     * The leader will retry the transfer on the next replication cycle. Repeated failures suggest a mismatch between
     * the leader's snapshot format and the follower's state machine, or persistent I/O errors on the follower.
     * </p>
     *
     * @return the number of failed snapshot installations, or {@code -1} if metrics are disabled.
     */
    int getFailedSnapshotInstallations();

    /**
     * The number of chunked snapshot transfers that started but failed before completion.
     *
     * <p>
     * A chunked transfer fails when a view change interrupts the transfer (leader steps down, follower leaves) or
     * when the leader takes a newer snapshot while a transfer is in progress. Occasional failures during membership
     * changes are expected. A high value on a stable cluster may indicate network issues between the leader and
     * this follower. Returns zero when synchronous snapshots are in use.
     * </p>
     *
     * @return the number of failed chunked transfers, or {@code -1} if metrics are disabled.
     */
    int getFailedSnapshotTransfers();

    /**
     * The duration of the last completed chunked snapshot transfer on this node.
     *
     * <p>
     * Measures wall-clock time from the first chunk request to the final chunk received. Use this metric to evaluate
     * whether the current chunk size and batch size configuration provides adequate transfer throughput. A transfer
     * that takes significantly longer than expected may benefit from larger chunks or batches. Returns
     * {@link Duration#ZERO} when no chunked transfer has completed or when synchronous snapshots are in use.
     * </p>
     *
     * @return the duration of the last chunked transfer, or {@link Duration#ZERO} if metrics are disabled or no
     *         transfer has completed.
     */
    Duration getLastSnapshotTransferDuration();
}
