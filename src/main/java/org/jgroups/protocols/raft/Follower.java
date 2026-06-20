package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.protocols.raft.internal.snapshot.SnapshotManager;

import java.nio.ByteBuffer;

/**
 * Implements the behavior of a RAFT follower
 * @author Bela Ban
 * @since  0.1
 */
public class Follower extends RaftImpl {
    public Follower(RAFT raft) {
        super(raft);
    }

    @Override
    public void handleInstallSnapshotRequest(Message msg, Address leader,
                                             long last_included_index, long last_included_term) {
        if (raft.state_machine == null) {
            raft.getLog().error("%s: no state machine set, cannot install snapshot", raft.getAddress());
            return;
        }
        SnapshotManager snapshotManager = raft.snapshotManager();
        if (snapshotManager == null) {
            raft.getLog().error("%s: snapshot manager not available to handle install", raft.getAddress());
            return;
        }

        Address sender=msg.src();
        try {
            // Read into state machine
            ByteBuffer sn=ByteBuffer.wrap(msg.getArray(), msg.getOffset(), msg.getLength());
            snapshotManager.install(sn, last_included_index, last_included_term, (lastIncludedIndex, lastIncludedTerm) -> {
                raft.commit_index = raft.last_appended = lastIncludedIndex;
                AppendResult result = new AppendResult(AppendResult.Result.OK, lastIncludedIndex)
                        .commitIndex(raft.commitIndex());
                Message ack = new EmptyMessage(leader)
                        .putHeader(raft.getId(), new AppendEntriesResponse(raft.currentTerm(), result));
                raft.getDownProtocol().down(ack);
            });
        } catch(Exception ex) {
            raft.getLog().error("%s: failed applying snapshot from %s: %s", raft.getAddress(), sender, ex);
        }
    }

}
