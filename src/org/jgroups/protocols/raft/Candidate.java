package org.jgroups.protocols.raft;

/**
 * Implements the behavior of a RAFT candidate
 * @author Bela Ban
 * @since  0.1
 */
public class Candidate extends RaftImpl {
    public Candidate(RAFT raft) {
        super(raft);
    }


    public void init() {
        super.init();
    }

}
