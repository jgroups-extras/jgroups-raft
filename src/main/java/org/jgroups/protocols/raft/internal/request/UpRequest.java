package org.jgroups.protocols.raft.internal.request;

import org.jgroups.Message;
import org.jgroups.protocols.raft.RaftHeader;

public record UpRequest(Message message, RaftHeader header) implements BaseRequest {

    @Override
    public void startUserOperation() { }

    @Override
    public void completeUserOperation() { }

    @Override
    public void startReplication() { }

    @Override
    public void completeReplication() { }
}
