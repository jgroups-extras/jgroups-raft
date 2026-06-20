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
     * Resets all metrics.
     */
    void reset();
}
