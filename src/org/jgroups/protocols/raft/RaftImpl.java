package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;

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
        raft.heartbeatReceived(true);
        raft.currentTerm(term);
    }

    protected void handleAppendEntriesResponse(Address src, int term) {

    }

    protected void handleVoteRequest(Address src, int term) {
        // todo: this needs to be done atomically (with lock)
        if(term > raft.current_term && raft.votedFor(src)) {
            sendVoteResponse(src, term); // raft.current_term);
        }
    }

    protected void handleVoteResponse(Address src, int term) {
        if(term == raft.current_term) {
            int num_votes;
            if((num_votes=raft.incrVotes()) >= raft.majority) {
                // we've got the majority: become leader
                raft.log().trace("%s: received majority (%d) or votes -> becoming leader", raft.local_addr, num_votes);
                raft.changeRole(RAFT.Role.Leader);
            }
        }
    }

    protected void handleInstallSnapshotRequest(Address src, int term) {

    }

    protected void handleInstallSnapshotResponse(Address src, int term) {

    }


    protected void runElection() {
        int new_term=raft.createNewTerm();

        raft.resetVotes();
        raft.incrVotes(); // I received my own vote

        // Vote for self - return if I already voted for someone else
        if(!raft.votedFor(raft.local_addr))
            return;

        // Send VoteRequest message
        sendVoteRequest(new_term);

        // Responses are received asynchronously. If majority -> become leader
    }


    protected void sendVoteRequest(int term) {
        VoteRequest req=new VoteRequest(term);
        raft.log().trace("%s: sending %s", raft.local_addr, req);
        Message vote_req=new Message(null).putHeader(raft.getId(), req);
        raft.getDownProtocol().down(new Event(Event.MSG, vote_req));
    }

    protected void sendVoteResponse(Address dest, int term) {
        VoteResponse rsp=new VoteResponse(term, true); // todo: send a negative response back, too
        raft.log().trace("%s: sending %s", raft.local_addr, rsp);
        Message vote_rsp=new Message(dest).putHeader(raft.getId(), rsp);
        raft.getDownProtocol().down(new Event(Event.MSG, vote_rsp));
    }
}
