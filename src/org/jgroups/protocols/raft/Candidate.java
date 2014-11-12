package org.jgroups.protocols.raft;

/**
 * Implements the behavior of a RAFT candidate
 * @author Bela Ban
 * @since  3.6
 */
public class Candidate extends RaftImpl {
    public Candidate(RAFT raft) {
        super(raft);
    }


    public void init() {
        super.init();
        runElection();
    }
}
