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
public class VoteRequest extends RaftHeader {
    protected int last_log_term;
    protected int last_log_index;


    public VoteRequest() {}
    public VoteRequest(int term, int last_log_term, int last_log_index) {
        super(term);
        this.last_log_term=last_log_term;
        this.last_log_index=last_log_index;
    }

    public int lastLogTerm()  {return last_log_term;}
    public int lastLogIndex() {return last_log_index;}

    public short getMagicId() {
        return ELECTION.VOTE_REQ;
    }

    public Supplier<? extends Header> create() {
        return VoteRequest::new;
    }

    public int serializedSize() {
        return super.serializedSize() + Bits.size(last_log_term) + Bits.size(last_log_index);
    }

    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeInt(last_log_term, out);
        Bits.writeInt(last_log_index, out);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        last_log_term=Bits.readInt(in);
        last_log_index=Bits.readInt(in);
    }

    public String toString() {
        return super.toString() + ", last_log_term=" + last_log_term + ", last_log_index=" + last_log_index;
    }
}
