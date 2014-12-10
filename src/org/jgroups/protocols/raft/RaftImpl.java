package org.jgroups.protocols.raft;

import org.jgroups.Address;

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
        raft.currentTerm(term);
        raft.leader(leader);
    }

    protected void handleAppendEntriesResponse(Address sender, int term, AppendResult result) {

    }

    protected void handleInstallSnapshotRequest(Address src, int term) {

    }

    protected void handleInstallSnapshotResponse(Address src, int term) {

    }
}
