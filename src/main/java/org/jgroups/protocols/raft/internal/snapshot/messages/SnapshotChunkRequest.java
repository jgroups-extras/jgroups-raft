package org.jgroups.protocols.raft.internal.snapshot.messages;

import org.jgroups.Header;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.protocols.raft.internal.snapshot.SnapshotConstant;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Header for a chunk batch request sent from a follower to the leader.
 *
 * <p>
 * The follower drives the transfer by requesting batches of chunks. Each request specifies a {@linkplain #startChunk()
 * starting chunk index} and a {@linkplain #count() count}, giving the leader everything it needs to serve the request
 * statelessly. Chunk offsets are deterministic: {@code chunkIndex * chunkSize}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotMetadataRequest
 * @see SnapshotChunkResponse
 */
public final class SnapshotChunkRequest extends RaftHeader {

    private long lastIncludedIndex;
    private int startChunk;
    private int count;

    public SnapshotChunkRequest() { }

    public SnapshotChunkRequest(long currTerm, long lastIncludedIndex, int startChunk, int count) {
        super(currTerm);
        this.lastIncludedIndex = lastIncludedIndex;
        this.startChunk = startChunk;
        this.count = count;
    }

    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int startChunk() {
        return startChunk;
    }

    public int count() {
        return count;
    }

    @Override
    public short getMagicId() {
        return SnapshotConstant.SNAPSHOT_CHUNK_REQ;
    }

    @Override
    public Supplier<? extends Header> create() {
        return SnapshotChunkRequest::new;
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Bits.size(lastIncludedIndex) + Bits.size(startChunk) + Bits.size(count);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeLongCompressed(lastIncludedIndex, out);
        Bits.writeIntCompressed(startChunk, out);
        Bits.writeIntCompressed(count, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        lastIncludedIndex = Bits.readLongCompressed(in);
        startChunk = Bits.readIntCompressed(in);
        count = Bits.readIntCompressed(in);
    }

    @Override
    public String toString() {
        return super.toString() + "[" +
                "lastIncludedIndex=" + lastIncludedIndex +
                ", startChunk=" + startChunk +
                ", count=" + count +
                ']';
    }
}
