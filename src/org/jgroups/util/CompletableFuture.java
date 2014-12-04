package org.jgroups.util;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Poor man's implementation of java.util.concurrent.CompletableFuture. Will be removed when switching to JDK 8.
 * @author Bela Ban
 * @since  0.1
 */
public class CompletableFuture<T> implements Future<T>, Condition {
    protected T                value;     // set in case of successful completion
    protected Throwable        exception; // set in case of failed completion
    protected boolean          done, cancelled;
    protected Function<T,Void> completion_handler;
    protected final Lock       lock=new ReentrantLock();
    protected final CondVar    cond_var=new CondVar(lock);

    public CompletableFuture() {
    }

    public CompletableFuture(Function<T,Void> completion_handler) {
        this.completion_handler=completion_handler;
    }

    public boolean complete(T value) {
        lock.lock();
        try {
            if(done)
                return false;
            done=true;
            this.value=value;
            if(completion_handler != null) {
                try {
                    completion_handler.apply(value);
                }
                catch(Throwable t) {}
            }
            cond_var.signal(true);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean completeExceptionally(Throwable ex) {
        lock.lock();
        try {
            if(done)
                return false;
            done=true;
            this.exception=ex;
            if(completion_handler != null) {
                try {
                    completion_handler.apply(exception);
                }
                catch(Throwable t) {}
            }
            cond_var.signal(true);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            if(cancelled)
                return false;
            cancelled=done=true;
            cond_var.signal(true);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        cond_var.waitFor(this);
        return _get();
    }

    @Override
    public T get(long timeout,TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean success=cond_var.waitFor(this, timeout,unit);
        if(!success)
            throw new TimeoutException();
        return _get();
    }

    @Override
    public boolean isMet() {
        return done;
    }

    protected T _get() throws InterruptedException, ExecutionException {
        if(cancelled)
            throw new CancellationException();
        if(exception != null)
            throw new ExecutionException(exception);
        return value;
    }
}
