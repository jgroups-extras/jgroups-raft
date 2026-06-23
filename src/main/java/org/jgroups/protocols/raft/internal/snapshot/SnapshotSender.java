package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;

import java.nio.ByteBuffer;

/**
 * Abstracts the transport for sending snapshots to remote nodes.
 *
 * <p>
 * Decouples snapshot managers from the JGroups protocol stack and message construction. The RAFT protocol provides the
 * concrete implementation when creating the snapshot manager via the {@link SnapshotManager#create} factory. This is done
 * mostly to help during testing. Actual implementation will always use JGroups through the RAFT instance.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
interface SnapshotSender {

    /**
     * Sends a complete snapshot to a remote node.
     *
     * @param dest the destination address
     * @param snapshot the serialized snapshot data
     * @param lastIndex the last committed log index reflected in the snapshot
     * @param lastTerm the term of the last committed log entry
     */
    void send(Address dest, ByteBuffer snapshot, long lastIndex, long lastTerm);

    /**
     * Sends snapshot metadata to a follower to initiate a chunked transfer.
     *
     * @param dest the follower's address
     * @param currTerm the leader's current term
     * @param lastIncludedIndex the last log index reflected in the snapshot
     * @param lastIncludedTerm the term of the last log entry in the snapshot
     * @param totalSize the total snapshot size in bytes
     */
    void sendMetadata(Address dest, long currTerm, long lastIncludedIndex, long lastIncludedTerm, long totalSize);

    /**
     * Requests a batch of snapshot chunks from the leader.
     *
     * @param dest the leader's address
     * @param currTerm the follower's current term
     * @param lastIncludedIndex identifies the index the snapshot was taken
     * @param startChunk the zero-based index of the first chunk to request
     * @param count the number of consecutive chunks to request
     */
    void sendChunkRequest(Address dest, long currTerm, long lastIncludedIndex, int startChunk, int count);

    /**
     * Delivers a snapshot chunk to a follower.
     *
     * @param dest the follower's address
     * @param currTerm the leader's current term
     * @param lastIncludedIndex identifies the index the snapshot was taken
     * @param chunk the raw chunk bytes
     * @param offset the byte offset within the full snapshot
     * @param done {@code true} if this is the last chunk in the snapshot
     */
    void sendChunkResponse(Address dest, long currTerm, long lastIncludedIndex, ByteBuffer chunk, long offset, boolean done);
}
