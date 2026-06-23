package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.protocols.raft.InstallSnapshotRequest;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkRequest;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkResponse;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotMetadataRequest;

import java.nio.ByteBuffer;

/**
 * JGroups transport implementation of {@link SnapshotSender}.
 *
 * <p>
 * Routes all snapshot-related messages through the RAFT protocol stack via {@link RAFT#getDownProtocol()}. Each method
 * constructs the appropriate header and message type, keeping message construction out of the snapshot managers.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotSender
 */
final class JGroupsRaftSnapshotSender implements SnapshotSender {
    private final RAFT raft;

    JGroupsRaftSnapshotSender(RAFT raft) {
        this.raft = raft;
    }

    @Override
    public void send(Address dest, ByteBuffer snapshot, long lastIndex, long lastTerm) {
        Message msg = new BytesMessage(dest, snapshot)
                .putHeader(raft.getId(), new InstallSnapshotRequest(raft.currentTerm(), raft.leader(), lastIndex, lastTerm));
        raft.getDownProtocol().down(msg);
    }

    @Override
    public void sendMetadata(Address dest, long currTerm, long lastIncludedIndex, long lastIncludedTerm, long totalSize) {
        Message msg = new EmptyMessage(dest)
                .putHeader(raft.getId(), new SnapshotMetadataRequest(raft.leader(), currTerm, lastIncludedTerm, lastIncludedIndex, totalSize));
        raft.getDownProtocol().down(msg);
    }

    @Override
    public void sendChunkRequest(Address dest, long currTerm, long lastIncludedIndex, int startChunk, int count) {
        Message msg = new EmptyMessage(dest)
                .putHeader(raft.getId(), new SnapshotChunkRequest(currTerm, lastIncludedIndex, startChunk, count));
        raft.getDownProtocol().down(msg);
    }

    @Override
    public void sendChunkResponse(Address dest, long currTerm, long lastIncludedIndex, ByteBuffer chunk, long offset, boolean done) {
        Message msg = new BytesMessage(dest, chunk)
                .putHeader(raft.getId(), new SnapshotChunkResponse(currTerm, lastIncludedIndex, offset, done));
        raft.getDownProtocol().down(msg);
    }
}
