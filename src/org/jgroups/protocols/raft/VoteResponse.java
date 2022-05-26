package org.jgroups.protocols.raft;

import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class VoteResponse extends RaftHeader {
    protected long last_log_term;  // term of the last log entry
    protected long last_log_index; // index of the last log entry

    public VoteResponse() {}
    public VoteResponse(long term, long last_log_term, long last_log_index) {
        super(term);
        this.last_log_term=last_log_term;
        this.last_log_index=last_log_index;
    }

    public short getMagicId() {
        return ELECTION.VOTE_RSP;
    }

    public Supplier<? extends Header> create() {
        return VoteResponse::new;
    }


    public int serializedSize() {
        return super.serializedSize() + Bits.size(last_log_term) + Bits.size(last_log_index);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        last_log_term=Bits.readLongCompressed(in);
        last_log_index=Bits.readLongCompressed(in);
    }

    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeLongCompressed(last_log_term, out);
        Bits.writeLongCompressed(last_log_index, out);
    }

    public String toString() {
        return super.toString() + ", last_log_term=" + last_log_term + ", last_log_index=" + last_log_index;
    }
}
