package org.jgroups.protocols.raft.internal.request;

import org.jgroups.Message;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.raft.Options;
import org.jgroups.raft.internal.metrics.RaftProtocolMetrics;
import org.jgroups.raft.util.TimeService;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Creates request instances for the RAFT event loop, wiring in metrics and time dependencies.
 *
 * @author José Bolina
 * @since 2.0
 */
public final class RequestFactory {
    private final TimeService timeService;
    private final RaftProtocolMetrics metrics;

    public RequestFactory(TimeService timeService, RaftProtocolMetrics metrics) {
        this.timeService = timeService;
        this.metrics = metrics;
    }

    /**
     * Creates an {@link UpRequest} for an incoming channel message.
     */
    public BaseRequest createUpRequest(Message message, RaftHeader raftHeader) {
        return new UpRequest(message, raftHeader);
    }

    /**
     * Creates a {@link DownRequest} for a user-submitted operation, with tracking if metrics are enabled.
     */
    public BaseRequest createDownRequest(CompletableFuture<byte[]> future,
                                         byte[] buffer,
                                         int offset,
                                         int length,
                                         boolean internal,
                                         Options options,
                                         boolean readOnly) {
        return DownRequest.create(future, buffer, offset, length, internal, options, readOnly, metrics, timeService);
    }

    /**
     * Creates a {@link CallableDownRequest} for an internal operation on the event loop.
     */
    public <T> BaseRequest createCallableRequest(Callable<T> callable, CompletableFuture<T> future) {
        return new CallableDownRequest<>(callable, future);
    }
}
