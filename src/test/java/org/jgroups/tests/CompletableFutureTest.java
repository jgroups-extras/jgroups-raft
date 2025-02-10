package org.jgroups.raft.tests;

import org.jgroups.Global;
import org.jgroups.util.Util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class CompletableFutureTest {
    protected CompletableFuture<Integer> f;

    @BeforeMethod protected void setup() {
        f=new CompletableFuture<>();}

    public void testDone() {
        assert !f.isDone();
        assert !f.isCancelled();
        f.cancel(true);
        assert f.isCancelled();
        assert f.isDone();
    }

    public void testGet() throws Exception {
        boolean success=f.complete(1);
        assert success;
        success=f.complete(2);
        assert !success;
        int result=f.get();
        assert result == 1;
        result=f.get(500, TimeUnit.MILLISECONDS);
        assert result == 1;
    }

    public void testGetWithException() throws Exception {
        f.completeExceptionally(new NullPointerException("booom"));
        try {
            f.get();
            assert false : "should have thrown an exception";
        }
        catch(ExecutionException ex) {
            System.out.println("received ExecutionException as expected: " + ex);
            assert ex.getCause() instanceof NullPointerException;
        }
    }

    public void testGetWithTimeout() throws Exception {
        try {
            f.get(50, TimeUnit.MILLISECONDS);
            assert false : "should have thrown a TimeoutException";
        }
        catch(TimeoutException ex) {
            System.out.println("received TimeoutException as expected: " + ex);
        }
    }

    public void testDelayedGet() throws Exception {
        Completer<Integer> completer=new Completer<>(f, 5, null, 500);
        completer.start();
        int result=f.get();
        System.out.println("result = " + result);
        assert result == 5;
    }

    public void testCancel() throws Exception {
        new Thread(() -> {Util.sleep(500); f.cancel(true);}).start();

        try {
            f.get();
            assert false : "should have thrown a CancellationException";
        }
        catch(CancellationException cex) {
            System.out.println("received CancellationException as expected: " + cex);
        }
        assert f.isCancelled() && f.isDone();
    }

    public void testCompletionHandler() throws Exception {
        CompletableFuture<Integer> fut=new CompletableFuture<>();
        new Completer<>(fut, 5, null, 500).start();

        Util.waitUntil(10000, 100, fut::isDone);
        assert fut.get(2, TimeUnit.SECONDS) == 5;
    }

    public void testCompletionHandlerWithException() throws TimeoutException {
        MyCompletionHandler<Integer> handler=new MyCompletionHandler<>();
        f=new CompletableFuture<>();
        f.whenComplete(handler);
        new Completer<>(f, 0, new NullPointerException("booom"), 50).start();
        Util.waitUntil(10000, 500, () -> f.isDone());
        Throwable ex=handler.getException();
        assert ex instanceof NullPointerException;
    }


    protected static class Completer<R> extends Thread {
        protected final CompletableFuture<R> future;
        protected final R                    result;
        protected final Throwable            t;
        protected final long                 sleep;

        public Completer(CompletableFuture<R> future, R result, Throwable t, long sleep) {
            this.future=future;
            this.result=result;
            this.t=t;
            this.sleep=sleep;
        }

        public void run() {
            Util.sleep(sleep);
            if(t != null)
                future.completeExceptionally(t);
            else
                future.complete(result);
        }
    }


    protected static class MyCompletionHandler<T> implements BiConsumer<T, Throwable> {
        protected T         value;
        protected Throwable ex;

        public T         getValue()     {return value;}
        public Throwable getException() {return ex;}

        public void accept(T t, Throwable ex) {
            if(t != null)
                value=t;
            if(ex != null)
                this.ex=ex;
        }
    }
}
