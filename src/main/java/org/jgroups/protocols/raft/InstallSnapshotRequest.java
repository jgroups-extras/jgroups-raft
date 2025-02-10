package org.jgroups.protocols.raft;

import org.jgroups.Address;
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
public class InstallSnapshotRequest extends RaftHeader {
    protected Address leader;
    protected long    last_included_index;
    protected long    last_included_term;

    public InstallSnapshotRequest() {}
    public InstallSnapshotRequest(long term) {super(term);}

    public InstallSnapshotRequest(long term, Address leader, long last_included_index, long last_included_term) {
        this(term);
        this.leader=leader;
        this.last_included_index=last_included_index;
        this.last_included_term=last_included_term;
    }

    public short getMagicId() {
        return RAFT.INSTALL_SNAPSHOT_REQ;
    }

    public Supplier<? extends Header> create() {
        return InstallSnapshotRequest::new;
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Util.size(leader) + Bits.size(last_included_index) + Bits.size(last_included_term);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Util.writeAddress(leader, out);
        Bits.writeLongCompressed(last_included_index, out);
        Bits.writeLongCompressed(last_included_term, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        leader=Util.readAddress(in);
        last_included_index=Bits.readLongCompressed(in);
        last_included_term=Bits.readLongCompressed(in);
    }

    @Override
    public String toString() {
        return super.toString() + ", leader=" + leader + ", last_included_index=" + last_included_index +
          ", last_included_term=" + last_included_term;
    }
}
