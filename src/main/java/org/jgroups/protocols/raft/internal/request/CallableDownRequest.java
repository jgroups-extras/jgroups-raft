package org.jgroups.protocols.raft.internal.request;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * An arbitrary operation submitted for single-threaded execution on the RAFT event loop.
 *
 * <p>
 * Unlike {@link DownRequest}, this does not go through log replication. Used for operations that require event loop
 * thread safety without consensus. No latency tracking is performed.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public final class CallableDownRequest<T> extends UntrackedRequest {
    private final Callable<T> callable;
    private final CompletableFuture<T> future;

    public CallableDownRequest(Callable<T> callable, CompletableFuture<T> future) {
        this.callable = callable;
        this.future = future;
    }

    /**
     * Fails the request, completing the future exceptionally.
     *
     * @return always {@code true}.
     */
    @Override
    public boolean failed(Throwable t) {
        future.completeExceptionally(t);
        return true;
    }

    /**
     * Executes the callable on the event loop thread and resolves the future with the result.
     * If the callable throws, the future is completed exceptionally.
     */
    public void complete() {
        try {
            T result = callable.call();
            future.complete(result);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public String toString() {
        return "CallableDownRequest[" +
                "callable=" + callable + ", " +
                "future=" + future + ']';
    }
}
