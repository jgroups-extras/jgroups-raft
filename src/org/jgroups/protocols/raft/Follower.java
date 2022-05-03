package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Util;

/**
 * Implements the behavior of a RAFT follower
 * @author Bela Ban
 * @since  0.1
 */
public class Follower extends RaftImpl {
    public Follower(RAFT raft) {super(raft);}

    @Override
    public void handleInstallSnapshotRequest(Message msg, Address leader,
                                             int last_included_index, int last_included_term) {
        StateMachine sm;
        if((sm=raft.state_machine) == null) {
            raft.getLog().error("%s: no state machine set, cannot install snapshot", raft.getAddress());
            return;
        }
        Address sender=msg.src();
        try {
            // Read into state machine
            ByteArray sn=new ByteArray(msg.getArray(), msg.getOffset(), msg.getLength());
            raft.log().setSnapshot(sn);
            sm.readContentFrom(new ByteArrayDataInputStream(sn.getArray(), sn.getOffset(), sn.getLength()));

            // insert a dummy entry at last_included_index and set first/last/commit to it
            Log log=raft.log();
            LogEntry le=new LogEntry(last_included_term, null);
            log.reinitializeTo(last_included_index, le);
            raft.commit_index=raft.last_appended=last_included_index;

            raft.getLog().debug("%s: applied snapshot (%s) from %s; last_appended=%d, commit_index=%d",
                                raft.getAddress(), Util.printBytes(msg.getLength()), msg.src(), raft.lastAppended(),
                                raft.commitIndex());
            raft.num_snapshot_received++;
            AppendResult result=new AppendResult(AppendResult.Result.OK, last_included_index).commitIndex(raft.commitIndex());
            Message ack=new EmptyMessage(leader).putHeader(raft.getId(), new AppendEntriesResponse(raft.currentTerm(), result));
            raft.getDownProtocol().down(ack);
        }
        catch(Exception ex) {
            raft.getLog().error("%s: failed applying snapshot from %s: %s", raft.getAddress(), sender, ex);
        }
    }

}
