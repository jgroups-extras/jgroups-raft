package org.jgroups.protocols.raft;

import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;

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


    public int size() {
        return super.size() + Bits.size(last_log_term) + Bits.size(last_log_index);
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Bits.writeInt(last_log_term, out);
        Bits.writeInt(last_log_index, out);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        last_log_term=Bits.readInt(in);
        last_log_index=Bits.readInt(in);
    }

    public String toString() {
        return super.toString() + ", last_log_term=" + last_log_term + ", last_log_index=" + last_log_index;
    }
}
