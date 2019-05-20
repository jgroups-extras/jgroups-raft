package org.jgroups.protocols.raft;

import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class VoteResponse extends RaftHeader {
    protected boolean result;

    public VoteResponse() {}
    public VoteResponse(int term, boolean result) {super(term); this.result=result;}

    public short getMagicId() {
        return ELECTION.VOTE_RSP;
    }

    public Supplier<? extends Header> create() {
        return VoteResponse::new;
    }

    public boolean result() {return result;}

    public int serializedSize() {
        return super.serializedSize() + 1;
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
    	result = in.readByte() == 1 ? true : false;
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        out.writeByte(result ? 1 : 0);
    }

    public String toString() {
        return super.toString() + ", result=" + result;
    }
}
