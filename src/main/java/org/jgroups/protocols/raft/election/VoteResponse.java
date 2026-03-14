package org.jgroups.protocols.raft.election;

import static org.jgroups.protocols.raft.election.BaseElection.VOTE_RSP;

import org.jgroups.Header;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class VoteResponse extends RaftHeader implements Comparable<VoteResponse> {
    protected long last_log_term;  // term of the last log entry
    protected long last_log_index; // index of the last log entry

    public VoteResponse() {}
    public VoteResponse(long term, long last_log_term, long last_log_index) {
        super(term);
        this.last_log_term=last_log_term;
        this.last_log_index=last_log_index;
    }

    @Override
    public short getMagicId() {
        return VOTE_RSP;
    }

    @Override
    public Supplier<? extends Header> create() {
        return VoteResponse::new;
    }

    /**
     * Compares the {@link VoteResponse}s in search of the highest one.
     *
     * <p>
     * The verification follows the precedence:
     * <ol>
     *     <li>Compare Raft terms;</li>
     *     <li>Compare the log last appended.</li>
     * </ol>
     * This verification ensures the decided leader has the longest log.
     * </p>
     *
     * @param other The candidate response to check.
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than
     *         the specified object.
     */
    @Override
    public int compareTo(VoteResponse other) {
        int termCompare = Long.compare(this.last_log_term, other.last_log_term);
        return termCompare == 0
                ? Long.compare(this.last_log_index, other.last_log_index)
                : termCompare;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VoteResponse other)) return false;
        return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(last_log_term) * 31 + Long.hashCode(last_log_index);
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Bits.size(last_log_term) + Bits.size(last_log_index);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        last_log_term=Bits.readLongCompressed(in);
        last_log_index=Bits.readLongCompressed(in);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeLongCompressed(last_log_term, out);
        Bits.writeLongCompressed(last_log_index, out);
    }

    @Override
    public String toString() {
        return super.toString() + ", last_log_term=" + last_log_term + ", last_log_index=" + last_log_index;
    }
}
