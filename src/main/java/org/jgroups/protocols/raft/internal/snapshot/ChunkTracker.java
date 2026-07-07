package org.jgroups.protocols.raft.internal.snapshot;

import java.util.BitSet;

/**
 * Tracks chunk reception and flow control for a single snapshot transfer.
 *
 * <p>
 * Maintains a {@link BitSet} of received chunks and a request frontier to drive sliding window refill decisions. All indices
 * are zero-based chunk ordinals derived from byte offsets and the configured chunk size.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
final class ChunkTracker {

    private static final int[] EMPTY_CHUNKS = {};

    private final BitSet received;
    private final int totalChunks;
    private final int chunkSize;
    private final int batchSize;
    private final int lowWaterMark;

    private int highestRequested;
    private long bytesReceived;

    public ChunkTracker(long totalSize, int chunkSize, int batchSize) {
        this.totalChunks = (int) Math.ceil((double) totalSize / chunkSize);
        this.chunkSize = chunkSize;
        this.batchSize = batchSize;
        this.lowWaterMark = batchSize >> 2;
        this.received = new BitSet(totalChunks);
    }

    public int totalChunks() {
        return totalChunks;
    }

    public int received() {
        return received.cardinality();
    }

    public int inFlight() {
        return highestRequested - received.cardinality();
    }

    public int highestRequested() {
        return highestRequested;
    }

    public long bytesReceived() {
        return bytesReceived;
    }

    public boolean isComplete() {
        return received.cardinality() == totalChunks;
    }

    public int[] missingChunks() {
        int missing = inFlight();
        if (missing == 0) {
            return EMPTY_CHUNKS;
        }

        int[] result = new int[missing];
        int idx = 0;
        for (int i = received.nextClearBit(0); i < highestRequested && idx < result.length; i = received.nextClearBit(i + 1)) {
            result[idx++] = i;
        }
        return result;
    }

    /**
     * Records a chunk as received by converting the byte offset to a chunk index.
     *
     * <p>
     * Byte count is always accumulated regardless of whether the index falls within bounds.
     * </p>
     *
     * @param offset byte offset of the chunk within the snapshot
     * @param bytes  number of bytes in this chunk
     */
    public void markReceived(long offset, int bytes) {
        int chunkIndex = (int) (offset / chunkSize);
        if (chunkIndex >= 0 && chunkIndex < totalChunks) {
            received.set(chunkIndex);
        }
        bytesReceived += bytes;
    }

    /**
     * Advances the request frontier by the given count, capped at {@link #totalChunks()}.
     *
     * @param count number of additional chunks to mark as requested
     */
    public void markRequested(int count) {
        highestRequested = Math.min(highestRequested + count, totalChunks);
    }

    /**
     * Determines whether new chunks should be requested from the leader.
     * Returns {@code true} when the number of in-flight chunks has dropped to the low water mark
     * and there are still unrequested chunks remaining.
     *
     * @return {@code true} if a refill request should be sent
     */
    public boolean shouldRefill() {
        return highestRequested < totalChunks && inFlight() <= lowWaterMark;
    }

    /**
     * Computes how many chunks to request in the next refill to bring the pipeline back to capacity.
     *
     * @return number of chunks to request, zero if no refill is needed
     */
    public int refillCount() {
        return Math.min(batchSize - inFlight(), totalChunks - highestRequested);
    }

    /**
     * Returns the chunk index where the next request batch should begin.
     *
     * @return the first unrequested chunk index
     */
    public int nextRequestStart() {
        return highestRequested;
    }
}
