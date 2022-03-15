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
    protected int curr_term; // the current term on the leader

    public RaftHeader() {}
    public RaftHeader(int curr_term) {this.curr_term=curr_term;}

    public int        currTerm()      {return curr_term;}
    public RaftHeader currTerm(int t) {curr_term=t; return this;}


    public int serializedSize() {
        return Bits.size(curr_term);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(curr_term);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        curr_term=in.readInt();
    }

    public String toString() {return getClass().getSimpleName() + ": current_term=" + curr_term;}
}
