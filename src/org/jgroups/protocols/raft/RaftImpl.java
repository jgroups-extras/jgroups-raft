package org.jgroups.protocols.raft;

import org.jgroups.Address;

/**
 * Base class for the different roles a RAFT node can have (follower, candidate, leader)
 * @author Bela Ban
 * @since  3.6
 */
public abstract class RaftImpl {
    protected RAFT raft; // a ref to the enclosing RAFT protocol

    public RaftImpl(RAFT raft) {this.raft=raft;}

    public RAFT     raft()       {return raft;}
    public RaftImpl raft(RAFT r) {this.raft=r; return this;}

    /** Called right after instantiation */
    public void init() {
        raft.startElectionTimer(); // for follower and candidate
    }

    /** Called before getting destroyed (on a role change) */
    public void destroy() {}

    /** Called when the election timeout elapsed */
    protected void electionTimeout() {
        raft.log().trace("%s: election timed out", raft.local_addr);
    }

    protected void handleAppendEntriesRequest(Address sender, int term) {

    }

    protected void handleAppendEntriesResponse(Address src, int term) {

    }

    protected void handleRequestVoteRequest(Address src, int term) {

    }

    protected void handleRequestVoteResponse(Address src, int term) {

    }

    protected void handleInstallSnapshotRequest(Address src, int term) {

    }

    protected void handleInstallSnapshotResponse(Address src, int term) {

    }


    protected void runElection() {
        raft.createNewTerm();
    }
}
