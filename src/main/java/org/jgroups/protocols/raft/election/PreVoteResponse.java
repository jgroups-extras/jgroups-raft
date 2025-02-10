package org.jgroups.protocols.raft.election;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Utilized during the pre-voting phase to return information about the current seen leader.
 *
 * @author José Bolina
 * @since 1.0.12
 */
public class PreVoteResponse extends RaftHeader {

    protected Address leader;

    public PreVoteResponse() {}

    public PreVoteResponse(Address leader) {
        this.leader = leader;
    }

    public Address leader() {
        return leader;
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        leader = Util.readAddress(in);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Util.writeAddress(leader, out);
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Util.size(leader);
    }

    @Override
    public short getMagicId() {
        return ELECTION2.PRE_VOTE_RSP;
    }

    @Override
    public Supplier<? extends Header> create() {
        return PreVoteResponse::new;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ": leader=" + leader;
    }
}
