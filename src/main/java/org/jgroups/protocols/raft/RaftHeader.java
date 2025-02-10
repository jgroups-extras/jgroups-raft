package org.jgroups.protocols.raft;

import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Bela Ban
 * @since  0.1
 */
public abstract class RaftHeader extends Header {
    protected long curr_term; // the current term on the leader

    public RaftHeader() {}
    public RaftHeader(long curr_term) {this.curr_term=curr_term;}

    public long       currTerm()       {return curr_term;}
    public RaftHeader currTerm(long t) {curr_term=t; return this;}


    public int serializedSize() {
        return Bits.size(curr_term);
    }

    public void writeTo(DataOutput out) throws IOException {
        Bits.writeLongCompressed(curr_term, out);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        curr_term=Bits.readLongCompressed(in);
    }

    public String toString() {return getClass().getSimpleName() + ": current_term=" + curr_term;}
}
