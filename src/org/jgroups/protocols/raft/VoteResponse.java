package org.jgroups.protocols.raft;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class VoteResponse extends RaftHeader {
    protected boolean result;

    public VoteResponse() {}
    public VoteResponse(int term, boolean result) {super(term); this.result=result;}


    public int size() {
        return super.size();
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
    }

    public String toString() {
        return super.toString() + ", result=" + result;
    }
}
