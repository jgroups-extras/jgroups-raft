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
 * Used to send AppendEntries messages to cluster members. The log entries are contained in actual payload of the message,
 * not in this header.
 * @author Bela Ban
 * @since  0.1
 */
public class AppendEntriesRequest extends RaftHeader {
    protected Address    leader;         // probably not needed as msg.src() contains the leader's address already

    // the term of the entry; this differs from term, e.g. when a LogEntry is resent with entry_term=25 and term=30
    protected long       entry_term;
    protected long       prev_log_index;
    protected long       prev_log_term;
    protected long       leader_commit;  // the commit_index of the leader

    public AppendEntriesRequest() {}
    public AppendEntriesRequest(Address leader, long current_term, long prev_log_index, long prev_log_term,
                                long entry_term, long leader_commit) {
        super(current_term);
        this.leader=leader;
        this.entry_term=entry_term;
        this.prev_log_index=prev_log_index;
        this.prev_log_term=prev_log_term;
        this.leader_commit=leader_commit;
    }

    public short getMagicId() {
        return RAFT.APPEND_ENTRIES_REQ;
    }

    public Supplier<? extends Header> create() {
        return AppendEntriesRequest::new;
    }

    @Override
    public int serializedSize() {
        return super.serializedSize() + Util.size(leader) + Bits.size(entry_term) + Bits.size(prev_log_index)
          + Bits.size(prev_log_term) + Bits.size(leader_commit);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        Util.writeAddress(leader, out);
        Bits.writeLongCompressed(entry_term, out);
        Bits.writeLongCompressed(prev_log_index, out);
        Bits.writeLongCompressed(prev_log_term, out);
        Bits.writeLongCompressed(leader_commit, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        leader=Util.readAddress(in);
        entry_term=Bits.readLongCompressed(in);
        prev_log_index=Bits.readLongCompressed(in);
        prev_log_term=Bits.readLongCompressed(in);
        leader_commit=Bits.readLongCompressed(in);
    }

    @Override public String toString() {
        return String.format("%s, leader=%s, entry_term=%d, prev_log_index=%d, prev_log_term=%d, leader_commit=%d",
                             super.toString(), leader, entry_term, prev_log_index, prev_log_term, leader_commit);
    }
}
