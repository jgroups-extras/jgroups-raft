package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Sent by the freshly elected leader to all members (-self)
 * @author Bela Ban
 * @since  1.0.6
 */
public class LeaderElected extends RaftHeader {
    protected Address leader;

    public LeaderElected() {
    }

    public LeaderElected(Address leader) {this.leader=leader;}

    public Address leader()   {return leader;}
    public short getMagicId() {
        return ELECTION.LEADER_ELECTED;
    }

    public Supplier<? extends Header> create() {
        return LeaderElected::new;
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
