package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Used by {@link org.jgroups.protocols.raft.ELECTION} to send heartbeats. Contrary to the RAFT paper, heartbeats are
 * not emulated with AppendEntriesRequests
 * @author Bela Ban
 * @since  0.1
 */
public class HeartbeatRequest extends RaftHeader {
    protected Address leader;

    public HeartbeatRequest() {}
    public HeartbeatRequest(int term, Address leader) {super(term); this.leader=leader;}

    public short getMagicId() {
        return ELECTION.HEARTBEAT_REQ;
    }

    public Supplier<? extends Header> create() {
        return HeartbeatRequest::new;
    }

    public int serializedSize() {
        return super.serializedSize() + Util.size(leader);
    }

    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Util.writeAddress(leader, out);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        leader=Util.readAddress(in);
    }

    public String toString() {
        return super.toString() + ", leader=" + leader;
    }
}
