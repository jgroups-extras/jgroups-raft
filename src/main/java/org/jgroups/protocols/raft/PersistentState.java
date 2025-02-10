package org.jgroups.protocols.raft;

import org.jgroups.Global;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Responsible for a node's internal state.
 *
 * <p>The data in this class is serialized and stored at the beginning of a snapshot file. This class does not
 * hold information that the {@link RAFT} protocol requires to be persistent, e.g., terms and votes.</p>
 *
 * @author Jose Bolina
 * @since 1.0.11
 */
public class PersistentState implements SizeStreamable {
    private final List<String> members = new ArrayList<>();

    public List<String> getMembers() {
        return new ArrayList<>(members);
    }

    public void setMembers(Collection<String> value) {
        members.clear();
        members.addAll(new HashSet<>(value));
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        int size=members.size();
        out.writeInt(size);
        for (String member : members) {
            out.writeUTF(member);
        }
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        int size=in.readInt();
        List<String> tmp = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            tmp.add(in.readUTF());
        }
        setMembers(tmp);
    }

    @Override
    public int serializedSize() {
        int size=Global.INT_SIZE;
        for (String member : members) {
            size += Util.size(member);
        }
        return size;
    }

    @Override
    public String toString() {
        return String.format("members=%s", members);
    }
}
