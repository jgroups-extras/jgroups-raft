package org.jgroups.protocols.raft;

import org.jgroups.Address;

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
        raft.createCommitTable();
        raft.startResendTask();
    }

    public void destroy() {
        super.destroy();
        raft.stopResendTask();
        raft.request_table=null;
        raft.commit_table=null;
    }


    @Override
    protected void handleAppendEntriesResponse(Address sender, int term, AppendResult result) {
        RAFT.RequestTable reqtab=raft.request_table;
        if(reqtab == null)
            throw new IllegalStateException("request table cannot be null in leader");
        if(result.success) {
            raft.commit_table.update(sender, result.getIndex(), result.getIndex()+1, result.commit_index);
            if(reqtab.add(result.index, sender))
                raft.handleCommit(result.index);
        }
        else
            raft.commit_table.update(sender, 0, result.getIndex(), result.commit_index);
    }

}
