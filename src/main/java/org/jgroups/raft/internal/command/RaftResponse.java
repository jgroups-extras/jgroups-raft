package org.jgroups.raft.internal.command;

import org.jgroups.raft.exceptions.JRaftException;
import org.jgroups.raft.internal.serialization.ObjectWrapper;
import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypes.RAFT_RESPONSE)
public final class RaftResponse {

    private final Object response;
    private final Throwable exception;

    @ProtoFactory
    RaftResponse(ObjectWrapper<Object> response, String exception) {
        this.response = ObjectWrapper.unwrap(response);
        this.exception = exception != null ? JRaftException.stackless(String.format("Failed apply command, see remote for stack trace: %s", exception)) : null;
    }

    private RaftResponse(Object response, Throwable exception) {
        this.response = response;
        this.exception = exception;
    }

    public static RaftResponse success(Object response) {
        return new RaftResponse(response, null);
    }

    public static RaftResponse failure(Throwable exception) {
        return new RaftResponse(null, exception);
    }

    @ProtoField(number = 1)
    ObjectWrapper<Object> getResponse() {
        return ObjectWrapper.create(response);
    }

    @ProtoField(number = 2)
    String getException() {
        return exception != null ? exception.getMessage() : null;
    }

    public boolean isSuccess() {
        return exception == null;
    }

    public Object response() {
        return response;
    }

    public Throwable exception() {
        return exception;
    }
}
