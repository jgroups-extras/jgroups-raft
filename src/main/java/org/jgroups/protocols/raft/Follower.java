package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.raft.StateMachine;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.nio.ByteBuffer;

/**
 * Implements the behavior of a RAFT follower
 * @author Bela Ban
 * @since  0.1
 */
public class Follower extends RaftImpl {
    public Follower(RAFT raft) {super(raft);}

    @Override
    public void handleInstallSnapshotRequest(Message msg, Address leader,
                                             long last_included_index, long last_included_term) {
        StateMachine sm;
        if((sm=raft.state_machine) == null) {
            raft.getLog().error("%s: no state machine set, cannot install snapshot", raft.getAddress());
            return;
        }
        Address sender=msg.src();
        try {
            // Read into state machine
            ByteBuffer sn=ByteBuffer.wrap(msg.getArray(), msg.getOffset(), msg.getLength());
            raft.log().setSnapshot(sn);

            DataInput in=new ByteArrayDataInputStream(msg.getArray(), msg.getOffset(), msg.getLength());
            raft.internal_state.readFrom(in);
            sm.readContentFrom(in);

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
