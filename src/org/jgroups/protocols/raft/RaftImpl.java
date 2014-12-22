package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;

/**
 * Base class for the different roles a RAFT node can have (follower, candidate, leader)
 * @author Bela Ban
 * @since  0.1
 */
public abstract class RaftImpl {
    protected RAFT raft; // a ref to the enclosing RAFT protocol

    public RaftImpl(RAFT raft) {this.raft=raft;}

    public RAFT     raft()       {return raft;}
    public RaftImpl raft(RAFT r) {this.raft=r; return this;}

    /** Called right after instantiation */
    public void init() {
    }

    /** Called before getting destroyed (on a role change) */
    public void destroy() {}


    /**
     * Called (on a follower) when an AppendEntries request is received
     * @param data The data (command to be appended to the log)
     * @param offset The offset
     * @param length The length
     * @param leader The leader's address (= the sender)
     * @param term The term of this append
     * @param prev_log_index The index of the previous log entry
     * @param prev_log_term The term of the previous log entry
     * @param leader_commit The leader's commit_index
     */
    protected void handleAppendEntriesRequest(byte[] data, int offset, int length, Address leader,
                                              int term, int prev_log_index, int prev_log_term, int leader_commit) {
        boolean commit=true;
        // todo: synchronize
        // todo: if term < current_term: ignore request
        raft.currentTerm(term);
        raft.leader(leader);

        if(data == null && length == 0) { // we got an empty AppendEntries message containing only leader_commit
            handleCommitRequest(leader, leader_commit);
            return;
        }

        AppendResult result=null;
        if(prev_log_index <= 0) { // special case: first entry
            commit=false;
            int curr_index=Math.max(prev_log_index+1, 1);
            result=new AppendResult(true, curr_index);
            raft.append(term, curr_index, data, offset, length);
        }
        else {
            LogEntry prev=raft.log_impl.get(prev_log_index);
            if(prev == null) {
                result=new AppendResult(false, raft.log_impl.lastApplied());
                commit=false;
            }
            else {
                // if the entry at prev_log_index has a different term than prev_term -> return false and the start index of
                // the conflictig term
                if(prev.term == prev_log_term) {
                    int curr_index=prev_log_index + 1;
                    LogEntry existing=raft.log_impl.get(curr_index);
                    if(existing != null) {
                        if(existing.term != term) {
                            // delete this and all subsequent entries and overwrite with received entry
                            raft.log_impl.deleteAllEntriesStartingFrom(curr_index);
                            result=new AppendResult(false, curr_index);
                            commit=false;
                        }
                        else { // received before and is identical
                            result=new AppendResult(true, curr_index);
                            commit=true;
                        }
                    }
                    else
                        result=new AppendResult(true, curr_index);

                    raft.append(term, curr_index, data, offset, length);
                }
                else {
                    result=new AppendResult(false, getFirstIndexOfConflictingTerm(prev_log_index, prev.term));
                    commit=false;
                }
            }
        }

        // commit entries up to leader_commit (if possible) and apply to state machine
        if(commit)
            raft.commitLogTo(leader_commit);

        // send AppendEntries response
        if(result != null) {
            result.commitIndex(raft.commitIndex());
            Message msg=new Message(leader).putHeader(raft.getId(), new AppendEntriesResponse(raft.currentTerm(), result));
            raft.getDownProtocol().down(new Event(Event.MSG, msg));
        }
    }


    protected void handleAppendEntriesResponse(Address sender, int term, AppendResult result) {
    }

    protected void handleInstallSnapshotRequest(Address src, int term) {
    }

    protected void handleInstallSnapshotResponse(Address src, int term) {
    }

    /** Finds the first index at which conflicting_term starts, going back from start_index towards the head of the log */
    protected int getFirstIndexOfConflictingTerm(int start_index, int conflicting_term) {
        Log log=raft.log_impl;
        int first=log.firstApplied(), last=log.lastApplied();
        int retval=Math.min(start_index, last);
        for(int i=retval; i >= first; i--) {
            LogEntry entry=log.get(i);
            if(entry == null)
                break;
            if(entry.term != conflicting_term)
                return i;
        }
        return retval;
    }

    protected void handleCommitRequest(Address sender, int leader_commit) {
        raft.commitLogTo(leader_commit);
        AppendResult result=new AppendResult(true, raft.lastApplied()).commitIndex(raft.commitIndex());
        Message msg=new Message(sender).putHeader(raft.getId(), new AppendEntriesResponse(raft.currentTerm(), result));
        raft.getDownProtocol().down(new Event(Event.MSG, msg));
    }
}
