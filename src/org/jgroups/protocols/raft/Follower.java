package org.jgroups.protocols.raft;

/**
 * Implements the behavior of a RAFT follower
 * @author Bela Ban
 * @since  3.6
 */
public class Follower extends RaftImpl {

    public Follower() {}
    public Follower(RAFT raft) {super(raft);}

    protected void electionTimeout() {
        super.electionTimeout();
        raft.changeRole(RAFT.Role.Candidate);
    }
}
