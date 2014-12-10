package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;

/**
 * Implements the behavior of a RAFT leader
 * @author Bela Ban
 * @since  0.1
 */
public class Leader extends RaftImpl {
    public Leader(RAFT raft) {
        super(raft);
    }


    public void init() {
        super.init();
        raft.request_table=new RAFT.RequestTable(raft.majority);
    }

    public void destroy() {
        super.destroy();
        raft.request_table=null;
    }


    @Override
    protected void handleAppendEntriesRequest(byte[] data, int offset, int length, Address leader,
                                              int term, int prev_log_index, int prev_log_term, int leader_commit) {
        super.handleAppendEntriesRequest(data, offset, length, leader, term, prev_log_index, prev_log_term, leader_commit);

        LogEntry entry=new LogEntry(term, data, offset, length);
        AppendResult result=raft.log_impl.append(prev_log_index, prev_log_term, entry);

        // send AppendEntries response
        Message msg=new Message(leader).putHeader(raft.getId(), new AppendEntriesResponse(raft.currentTerm(), result));
        raft.getDownProtocol().down(new Event(Event.MSG, msg));
    }

    @Override
    protected void handleAppendEntriesResponse(Address sender, int term, AppendResult result) {
        RAFT.RequestTable reqtab=raft.request_table;
        if(reqtab == null)
            throw new IllegalStateException("request table cannot be null in leader");
        if(result.success) {
            if(reqtab.add(result.index, sender)) {
                raft.handleCommit(result.index);
            }
        }
        else {
            // update commit_table: set nextIndex for sender to result.index
        }
    }

}
