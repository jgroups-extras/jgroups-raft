package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.ByteArrayDataInputStream;

/**
 * Implements the behavior of a RAFT follower
 * @author Bela Ban
 * @since  0.1
 */
public class Follower extends RaftImpl {
    public Follower(RAFT raft) {super(raft);}

    @Override
    protected void handleInstallSnapshotRequest(Message msg, int term, Address leader,
                                                int last_included_index, int last_included_term) {
        // 1. read the state (in the message's buffer) and apply it to the state machine (clear the SM before?)

        // 2. Delete the log (if it exists) and create a new log. Append a dummy entry at last_included_index with an
        //    empty buffer and term=last_included_term
        //    - first_applied=last_applied=commit_index=last_included_index

        StateMachine sm;
        if((sm=raft.state_machine) == null) {
            raft.getLog().error("%s: no state machine set, cannot install snapshot", raft.local_addr);
            return;
        }
        Address sender=msg.src();
        try {
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            sm.readContentFrom(in);

            // todo: delete and re-init log ?


            // insert a dummy entry
            Log log=raft.log();
            log.append(last_included_index, true, new LogEntry(last_included_term, null));
            raft.last_applied=last_included_index;
            log.commitIndex(last_included_index);
            raft.commit_index=last_included_index;


            // todo: set commit_index, last_applied, first_applied
            log.truncate(last_included_index);
        }
        catch(Exception ex) {
            raft.getLog().error("%s: failed applying snapshot from %s: %s", raft.local_addr, sender, ex);
        }
    }
}
