package org.jgroups.protocols.raft.internal.request;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public final class CallableDownRequest<T> implements BaseRequest {
    private final Callable<T> callable;
    private final CompletableFuture<T> future;

    public CallableDownRequest(Callable<T> callable, CompletableFuture<T> future) {
        this.callable = callable;
        this.future = future;
    }

    @Override
    public boolean failed(Throwable t) {
        future.completeExceptionally(t);
        return true;
    }

    public void complete() {
        try {
            T result = callable.call();
            future.complete(result);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public void startUserOperation() { }

    @Override
    public void completeUserOperation() { }

    @Override
    public void startReplication() { }

    @Override
    public void completeReplication() { }

    @Override
    public String toString() {
        return "CallableDownRequest[" +
                "callable=" + callable + ", " +
                "future=" + future + ']';
    }
}
