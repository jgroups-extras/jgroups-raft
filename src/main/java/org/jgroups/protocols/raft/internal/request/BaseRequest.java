package org.jgroups.protocols.raft.internal.request;

public sealed interface BaseRequest permits CallableDownRequest, DownRequest, UpRequest {

    void startUserOperation();

    void completeUserOperation();

    void startReplication();

    void completeReplication();

    default boolean failed(Throwable t) {
        return false;
    }
}
