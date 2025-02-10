package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Message;

import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_CONFLICTING_PREV_TERM;
import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_ENTRY_NOT_FOUND;
import static org.jgroups.protocols.raft.AppendResult.Result.OK;

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
     * @param entries The data (commands to be appended to the log)
     * @param leader The leader's address (= the sender)
     * @param prev_index The index of the previous log entry
     * @param prev_term The term of the previous log entry
     * @param entry_term The term of the entry
     * @param leader_commit The leader's commit_index
     * @return AppendResult A result (true or false), or null if the request was ignored (e.g. due to lower term)
     */
    public AppendResult handleAppendEntriesRequest(LogEntries entries, Address leader,
                                                   long prev_index, long prev_term, long entry_term, long leader_commit) {
        raft.leader(leader);
        long curr_index=prev_index+1;
        // we got an empty AppendEntries message containing only leader_commit, or the index is below the commit index
        if(entries == null || curr_index <= raft.commitIndex()) {
            raft.commitLogTo(leader_commit, false);
            return new AppendResult(OK, raft.lastAppended()).commitIndex(raft.commitIndex());
        }

        LogEntry prev=raft.log_impl.get(prev_index);
        if(prev == null && prev_index > 0) { // didn't find entry
            raft.num_failed_append_requests_not_found++;
            return new AppendResult(FAIL_ENTRY_NOT_FOUND, raft.lastAppended()); // required, e.g. when catching up as a new mbr
        }

        // if the term at prev_index != prev_term -> return false and the start index of the conflicting term
        if(prev_index == 0 || prev.term == prev_term) {
            LogEntry existing=raft.log_impl.get(curr_index);
            if(existing != null && existing.term != entry_term) {
                // delete this and all subsequent entries and overwrite with received entry
                raft.deleteAllLogEntriesStartingFrom(curr_index);
            }
            boolean added=raft.append(curr_index, entries);
            int num_added=added? entries.size() : 0;
            raft.commitLogTo(leader_commit, false);
            raft.num_successful_append_requests+=num_added;
            return new AppendResult(OK, added? prev_index+num_added : raft.lastAppended())
              .commitIndex(raft.commitIndex());
        }
        raft.num_failed_append_requests_wrong_term++;
        long conflicting_index=getFirstIndexOfConflictingTerm(prev_index, prev.term);
        if(conflicting_index <= raft.commitIndex()) {
            raft.getLog().error("%s: cannot delete entries <= %d as commit_index is higher: log=%s",
                                raft.getAddress(), conflicting_index, raft.log_impl);
            conflicting_index=raft.last_appended;
        }
        else
            raft.deleteAllLogEntriesStartingFrom(conflicting_index);
        return new AppendResult(FAIL_CONFLICTING_PREV_TERM, conflicting_index, prev.term).commitIndex(raft.commitIndex());
    }

    public void handleAppendEntriesResponse(Address sender, long term, AppendResult result) {
    }

    /**
     * Called when a snapshot sent by the leader is received by a follower. The follower needs to overwrite its
     * snapshot file with the received bytes and reinitialize its log to last_inluded_index and create a dummy
     * LogEntry with last_included_term at last_included_index. This is needed by the next
     * {@link RaftImpl#handleAppendEntriesRequest(LogEntries, Address, int, int, int, int)} call to compare to
     * the previous index and term.
     * @param msg The snapshot message sent by the leader
     * @param leader The leader (= sender of the message)
     * @param last_included_index The last index that's reflected in the state (=snapshot) sent by the leader. Indices
     *                            first_appended, commit_index and last_appended have to be set to last_included_index
     * @param last_included_term The last included term. The dummy entry needs to have this term, for comparison
     *                           in the next handleAppendRequest() call.
     */
    public void handleInstallSnapshotRequest(Message msg, Address leader, long last_included_index, long last_included_term) {

    }


    /** Finds the first index at which conflicting_term starts, going back from start_index towards the head of the log,
     * not not going lower than commit-index */
    protected long getFirstIndexOfConflictingTerm(long start_index, long conflicting_term) {
        Log log=raft.log_impl;
        long first=Math.max(1, log.firstAppended()), last=log.lastAppended(), commit_index=log.commitIndex();
        long retval=Math.min(start_index, last);
        for(long i=retval; i >= first && i > commit_index; i--) {
            LogEntry entry=log.get(i);
            if(entry == null || entry.term != conflicting_term)
                break;
            retval=i;
        }
        return retval;
    }


}
