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
 * Header for a snapshot chunk delivered from the leader to a follower.
 *
 * <p>
 * The message body carries the raw chunk bytes. The follower writes them directly to a temporary file at the position given
 * by {@link #offset()}, without inspecting or deserializing the data.
 * </p>
 *
 * <p>
 * When {@link #isDone()} returns {@code true}, this is the last chunk in the snapshot. The follower can then proceed to
 * verify the file and install the snapshot into the state machine.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotMetadataRequest
 * @see SnapshotChunkRequest
 */
public final class SnapshotChunkResponse extends RaftHeader {

    private long lastIncludedIndex;
    private long offset;
    private boolean done;

    public SnapshotChunkResponse() { }

    public SnapshotChunkResponse(long currTerm, long lastIncludedIndex, long offset, boolean done) {
        super(currTerm);
        this.lastIncludedIndex = lastIncludedIndex;
        this.offset = offset;
        this.done = done;
    }

    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long offset() {
        return offset;
    }

    public boolean isDone() {
        return done;
    }

    @Override
    public short getMagicId() {
        return SnapshotConstant.SNAPSHOT_CHUNK_RSP;
    }

    @Override
    public Supplier<? extends Header> create() {
        return SnapshotChunkResponse::new;
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Bits.size(lastIncludedIndex) + Bits.size(offset) + 1;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeLongCompressed(lastIncludedIndex, out);
        Bits.writeLongCompressed(offset, out);
        out.writeBoolean(done);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        lastIncludedIndex = Bits.readLongCompressed(in);
        offset = Bits.readLongCompressed(in);
        done = in.readBoolean();
    }

    @Override
    public String toString() {
        return super.toString() + "[" +
                "lastIncludedIndex=" + lastIncludedIndex +
                ", offset=" + offset +
                ", done=" + done +
                ']';
    }
}
