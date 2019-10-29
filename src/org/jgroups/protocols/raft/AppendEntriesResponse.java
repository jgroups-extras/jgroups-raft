package org.jgroups.protocols.raft;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class AppendEntriesResponse extends RaftHeader {
    protected AppendResult result;

    public AppendEntriesResponse() {}
    public AppendEntriesResponse(int term, AppendResult result) {super(term); this.result=result;}

    public short getMagicId() {
        return RAFT.APPEND_ENTRIES_RSP;
    }

    public Supplier<? extends Header> create() {
        return AppendEntriesResponse::new;
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Global.BYTE_SIZE + (result != null? result.size() : 0);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Util.writeStreamable(result, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        result=Util.readStreamable(AppendResult::new, in);
    }

    @Override
    public String toString() {
        return super.toString() + ", result: " + result;
    }
}
