package org.jgroups.protocols.raft.internal.request;

import org.jgroups.Message;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.raft.Options;
import org.jgroups.raft.internal.metrics.SystemMetricsTracker;
import org.jgroups.raft.util.TimeService;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public final class RequestFactory {
    private final TimeService timeService;
    private final SystemMetricsTracker tracker;

    public RequestFactory(TimeService timeService, SystemMetricsTracker tracker) {
        this.timeService = timeService;
        this.tracker = tracker;
    }

    public BaseRequest createUpRequest(Message message, RaftHeader raftHeader) {
        return new UpRequest(message, raftHeader);
    }

    public BaseRequest createDownRequest(CompletableFuture<byte[]> future,
                                         byte[] buffer,
                                         int offset,
                                         int length,
                                         boolean internal,
                                         Options options,
                                         boolean readOnly) {
        return DownRequest.create(future, buffer, offset, length, internal, options, readOnly, tracker, timeService);
    }

    public <T> BaseRequest createCallableRequest(Callable<T> callable, CompletableFuture<T> future) {
        return new CallableDownRequest<>(callable, future);
    }
}
