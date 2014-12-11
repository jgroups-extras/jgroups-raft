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
    }

    public void destroy() {
        super.destroy();
        raft.request_table=null;
    }


    @Override
    protected void handleAppendEntriesResponse(Address sender, int term, AppendResult result) {
        System.out.println("-- response from " + sender + ": " + result);
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
