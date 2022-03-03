package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.util.Util;

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
        raft.createRequestTable();
        raft.createCommitTable();
    }

    public void destroy() {
        super.destroy();
        raft.request_table=null;
        raft.commit_table=null;
    }


    @Override
    public void handleAppendEntriesResponse(Address sender, int term, AppendResult result) {
        RequestTable<String> reqtab=raft.request_table;
        if(reqtab == null)
            throw new IllegalStateException("request table cannot be null in leader");
        ExtendedUUID uuid=(ExtendedUUID)sender;
        String sender_raft_id=Util.bytesToString(uuid.get(RAFT.raft_id_key));
        raft.getLog().trace("%s: received AppendEntries response from %s for term %d: %s", raft.getAddress(), sender, term, result);
        switch(result.result) {
            case OK:
                raft.commit_table.update(sender, result.index(), result.index() + 1, result.commit_index, false);
                if(reqtab.add(result.index, sender_raft_id, raft.majority())) {
                    raft.handleCommit(result.index);
                    if(raft.send_commits_immediately)
                        sendCommitMessageToFollowers();
                }
                break;
                // todo: change
            case FAIL_ENTRY_NOT_FOUND:
                raft.commit_table.update(sender, result.index(), result.index()+1, result.commit_index, true);
                break;
            case FAIL_CONFLICTING_PREV_TERM:
                raft.commit_table.update(sender, result.index()-1, result.index(), result.commit_index, true, true);
                break;
        }
    }

    private void sendCommitMessageToFollowers() {
        raft.commit_table.forEach(this::sendCommitMessageToFollower);
    }

    private void sendCommitMessageToFollower(Address member, CommitTable.Entry entry) {
        if(raft.commit_index > entry.commitIndex()) {
            int cterm=raft.currentTerm();
            Message msg=new EmptyMessage(member)
              .putHeader(raft.getId(), new AppendEntriesRequest(raft.getAddress(), cterm, 0, 0, cterm, raft.commit_index, false));
            raft.down(msg);
        }
    }
}
