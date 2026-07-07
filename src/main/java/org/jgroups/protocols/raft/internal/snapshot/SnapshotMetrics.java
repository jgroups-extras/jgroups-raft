package org.jgroups.protocols.raft.internal.snapshot;

/**
 * Read-only view of snapshot-related counters.
 *
 * <p>
 * Exposed via {@link SnapshotManager#metrics()} so RAFT can surface counters through JMX without accessing raw fields.
 * The concrete implementation tracks mutations internally; only the snapshot manager increments counters.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public sealed interface SnapshotMetrics permits DefaultSnapshotMetrics {

    /**
     * Number of snapshots taken in this node.
     *
     * @return the number of snapshots taken in this node.
     */
    int numSnapshots();

    /**
     * Received snapshots.
     *
     * @return the number of snapshots received in this node.
     */
    int numSnapshotsReceived();

    /**
     * Number of times it failed to take a snapshot.
     *
     * @return the number of failed attempts taking a snapshot.
     */
    int numFailedSnapshotsTaken();

    /**
     * Number of time it failed to install a snapshot.
     *
     * @return the number of failed attempts installing a snapshot.
     */
    int numFailedSnapshotsInstalled();

    /**
     * Total snapshot chunks received across all chunked transfers.
     *
     * @return the number of chunks received, zero for synchronous managers
     */
    int numChunksReceived();

    /**
     * Total snapshot bytes received across all chunked transfers.
     *
     * @return the number of bytes received, zero for synchronous managers
     */
    long numBytesReceived();

    /**
     * Chunked transfers that started but failed before completion.
     *
     * @return the number of failed chunk transfers, zero for synchronous managers
     */
    int numFailedChunkTransfers();

    /**
     * Average interval between consecutive chunk arrivals in nanoseconds.
     *
     * @return the average inter-chunk interval, zero if no chunks received
     */
    long avgChunkIntervalNanos();

    /**
     * Duration of the last completed chunked transfer in nanoseconds.
     *
     * @return the last transfer duration, zero if no transfer completed
     */
    long lastTransferDurationNanos();

    /**
     * Total chunks expected in the active chunked transfer.
     *
     * @return the total chunk count, zero when no transfer is active
     */
    int activeTransferTotalChunks();

    /**
     * Chunks received so far in the active chunked transfer.
     *
     * @return the received chunk count, zero when no transfer is active
     */
    int activeTransferChunksReceived();

    /**
     * Chunks requested but not yet received in the active transfer.
     *
     * @return the in-flight chunk count, zero when no transfer is active
     */
    int activeTransferChunksInFlight();

    /**
     * Highest chunk index requested (exclusive) in the active transfer.
     *
     * @return the request frontier, zero when no transfer is active
     */
    int activeTransferHighestRequested();

    /**
     * Indices of chunks that have been requested but not yet received.
     * Returns a fresh copy on each invocation.
     *
     * @return the missing chunk indices, empty when no transfer is active
     */
    int[] activeTransferMissingChunks();

    /**
     * Resets all metrics.
     */
    void reset();
}
