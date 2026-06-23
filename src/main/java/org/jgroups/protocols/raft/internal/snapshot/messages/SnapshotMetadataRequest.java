package org.jgroups.protocols.raft.internal.snapshot.messages;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.protocols.raft.internal.snapshot.SnapshotConstant;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Header for snapshot metadata sent from a leader to a lagging follower.
 *
 * <p>
 * Initiates a chunked snapshot transfer. The leader sends this on every resend interval while
 * {@code nextIndex < firstAppended()}, carrying the snapshot's identity and size so the follower can prepare a temporary
 * file and begin requesting chunks.
 * </p>
 *
 * <p>
 * No data payload accompanies this header. The follower uses {@link #totalSize()} together with the configured chunk size
 * to compute the total number of chunks and issue the first {@link SnapshotChunkRequest}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotChunkRequest
 * @see SnapshotChunkResponse
 */
public final class SnapshotMetadataRequest extends RaftHeader {

    private Address leader;
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private long totalSize;

    public SnapshotMetadataRequest() { }

    public SnapshotMetadataRequest(Address leader, long currentTerm, long lastIncludedTerm, long lastIncludedIndex, long totalSize) {
        super(currentTerm);
        this.leader = leader;
        this.lastIncludedTerm = lastIncludedTerm;
        this.lastIncludedIndex = lastIncludedIndex;
        this.totalSize = totalSize;
    }

    public Address leader() {
        return leader;
    }

    public long lastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long lastIncludedTerm() {
        return lastIncludedTerm;
    }

    public long totalSize() {
        return totalSize;
    }

    @Override
    public short getMagicId() {
        return SnapshotConstant.SNAPSHOT_METADATA_REQ;
    }

    @Override
    public Supplier<? extends Header> create() {
        return SnapshotMetadataRequest::new;
    }

    @Override
    public int serializedSize() {
        return super.serializedSize()
                + Util.size(leader)
                + Bits.size(lastIncludedTerm) + Bits.size(lastIncludedIndex) + Bits.size(totalSize);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Util.writeAddress(leader, out);
        Bits.writeLongCompressed(lastIncludedTerm, out);
        Bits.writeLongCompressed(lastIncludedIndex, out);
        Bits.writeLongCompressed(totalSize, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        leader = Util.readAddress(in);
        lastIncludedTerm = Bits.readLongCompressed(in);
        lastIncludedIndex = Bits.readLongCompressed(in);
        totalSize = Bits.readLongCompressed(in);
    }

    @Override
    public String toString() {
        return super.toString() + "[" +
                "leader=" + leader +
                ", lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", totalSize=" + totalSize +
                ']';
    }
}
