package org.jgroups.protocols.raft.internal.request;

import org.jgroups.Message;
import org.jgroups.protocols.raft.RaftHeader;

/**
 * An internal message coming up the JGroups channel to be handled by the RAFT protocol.
 *
 * @author José Bolina
 * @since 2.0
 */
public final class UpRequest extends UntrackedRequest {
    private final Message message;
    private final RaftHeader header;

    public UpRequest(Message message, RaftHeader header) {
        this.message = message;
        this.header = header;
    }

    /**
     * @return the JGroups message received from the channel.
     */
    public Message message() {
        return message;
    }

    /**
     * @return the Raft-specific header attached to the message.
     */
    public RaftHeader header() {
        return header;
    }

    @Override
    public String toString() {
        return "UpRequest[" +
                "message=" + message + ", " +
                "header=" + header + ']';
    }
}
