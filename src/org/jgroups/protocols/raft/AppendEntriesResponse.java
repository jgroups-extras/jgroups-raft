package org.jgroups.protocols.raft;

import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class AppendEntriesResponse extends RaftHeader {
    protected AppendResult result;

    public AppendEntriesResponse() {}
    public AppendEntriesResponse(int term, AppendResult result) {super(term); this.result=result;}

    @Override
    public int size() {
        return super.size() +
          Global.BYTE_SIZE + (result != null? result.size() : 0);
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Util.writeStreamable(result, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        result=(AppendResult)Util.readStreamable(AppendResult.class, in);
    }

    @Override
    public String toString() {
        return super.toString() + ", result: " + result;
    }
}
