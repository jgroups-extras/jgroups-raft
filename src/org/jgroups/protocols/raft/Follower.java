package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

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
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(msg.getArray(), msg.getOffset(), msg.getLength());
            sm.readContentFrom(in);

            // Write to snapshot, replace existing file is present
            writeSnapshotTo(raft.snapshotName(), msg.getArray(), msg.getOffset(), msg.getLength());

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

    protected static void writeSnapshotTo(String snapshot_name, byte[] buf, int offset, int length) throws IOException {
        try(OutputStream output=new FileOutputStream(snapshot_name)) {
            output.write(buf, offset, length);
        }
    }
}
