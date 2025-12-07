package org.jgroups.protocols.raft.internal.request;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public record CallableDownRequest<T>(Callable<T> callable, CompletableFuture<T> future) implements BaseRequest {

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
}
