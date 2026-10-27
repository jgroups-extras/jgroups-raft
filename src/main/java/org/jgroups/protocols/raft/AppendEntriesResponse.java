package org.jgroups.protocols.raft;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
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
    private long round;

    public AppendEntriesResponse() {}
    public AppendEntriesResponse(long term, AppendResult result) {super(term); this.result=result;}

    public long round() {
        return round;
    }

    public AppendEntriesResponse round(long round) {
        this.round = round;
        return this;
    }

    @Override
    public short getMagicId() {
        return RAFT.APPEND_ENTRIES_RSP;
    }

    @Override
    public Supplier<? extends Header> create() {
        return AppendEntriesResponse::new;
    }

    @Override
    public int serializedSize() {
        return super.serializedSize()
                + Global.BYTE_SIZE
                + (result != null? result.size() : 0)
                + Bits.size(round);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Bits.writeLongCompressed(round, out);
        Util.writeStreamable(result, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        round = Bits.readLongCompressed(in);
        result=Util.readStreamable(AppendResult::new, in);
    }

    @Override
    public String toString() {
        return super.toString() + ", round=" + round + ", result: " + result;
    }
}
