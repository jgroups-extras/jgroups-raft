package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.Address;

import java.nio.ByteBuffer;

/**
 * Abstracts the transport for sending snapshots to remote nodes.
 *
 * <p>
 * Decouples snapshot managers from the JGroups protocol stack and message construction. The RAFT protocol provides the
 * concrete implementation when creating the snapshot manager via the {@link SnapshotManager#create} factory. This is done
 * mostly to help during testing. Actual implementation will always use JGroups through the RAFT instance.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */

@FunctionalInterface
interface SnapshotSender {

    /**
     * Sends a snapshot to a remote node.
     *
     * @param dest the destination address
     * @param snapshot the serialized snapshot data
     * @param lastIndex the last committed log index reflected in the snapshot
     * @param lastTerm the term of the last committed log entry
     * @throws Exception if the message cannot be sent
     */
    void send(Address dest, ByteBuffer snapshot, long lastIndex, long lastTerm) throws Exception;
}
