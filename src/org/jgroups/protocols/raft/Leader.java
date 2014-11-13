package org.jgroups.protocols.raft;

/**
 * Implements the behavior of a RAFT leader
 * @author Bela Ban
 * @since  3.6
 */
public class Leader extends RaftImpl {
    public Leader(RAFT raft) {
        super(raft);
    }


    public void init() {
        raft.stopElectionTimer();
        raft.startHeartbeatTimer();
    }

    public void destroy() {
        super.destroy();
        raft.stopHeartbeatTimer();
    }
}
