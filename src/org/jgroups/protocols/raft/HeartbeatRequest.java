package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;

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

    public int size() {
        return super.size() + Util.size(leader);
    }

    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Util.writeAddress(leader, out);
    }

    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        leader=Util.readAddress(in);
    }

    public String toString() {
        return super.toString() + ", leader=" + leader;
    }
}
