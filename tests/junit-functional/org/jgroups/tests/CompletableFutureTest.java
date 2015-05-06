package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.*;
import java.util.function.BiConsumer;

@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class CompletableFutureTest {
    protected CompletableFuture<Integer> future;

    @BeforeMethod protected void setup() {future=new CompletableFuture<>();}

    public void testDone() {
        assert !future.isDone();
        assert !future.isCancelled();
        future.cancel(true);
        assert future.isCancelled();
        assert future.isDone();
    }

    public void testGet() throws Exception {
        boolean success=future.complete(1);
        assert success;
        success=future.complete(2);
        assert !success;
        int result=future.get();
        assert result == 1;
        result=future.get(500,TimeUnit.MILLISECONDS);
        assert result == 1;
    }

    public void testGetWithException() throws Exception {
        future.completeExceptionally(new NullPointerException("booom"));
        try {
            future.get();
            assert false : "should have thrown an exception";
        }
        catch(ExecutionException ex) {
            System.out.println("received ExecutionException as expected: " + ex);
            assert ex.getCause() instanceof NullPointerException;
        }
    }

    public void testGetWithTimeout() throws Exception {
        try {
            future.get(50,TimeUnit.MILLISECONDS);
            assert false : "should have thrown a TimeoutException";
        }
        catch(TimeoutException ex) {
            System.out.println("received TimeoutException as expected: " + ex);
        }
    }

    public void testDelayedGet() throws Exception {
        Completer<Integer> completer=new Completer<>(future, 5, null, 500);
        completer.start();
        int result=future.get();
        System.out.println("result = " + result);
        assert result == 5;
    }

    public void testCancel() throws Exception {
        new Thread(() -> {Util.sleep(500); future.cancel(true);}).start();

        try {
            future.get();
            assert false : "should have thrown a CancellationException";
        }
        catch(CancellationException cex) {
            System.out.println("received CancellationException as expected: " + cex);
        }
        assert future.isCancelled() && future.isDone();
    }

    public void testCompletionHandler() {
        MyCompletionHandler<Integer> handler=new MyCompletionHandler<>();
        future=new CompletableFuture<>();
        future.whenComplete(handler);
        new Completer<>(future, 5, null, 500).start();

        for(int i=0; i < 20; i++) {
            if(future.isDone())
                break;
            Util.sleep(500);
        }

        assert handler.getException() == null;
        assert handler.getValue() == 5;
    }

    public void testCompletionHandlerWithException() {
        MyCompletionHandler<Integer> handler=new MyCompletionHandler<>();
        future=new CompletableFuture<>();
        future.whenComplete(handler);
        new Completer<>(future, 0, new NullPointerException("booom"), 500).start();

        for(int i=0; i < 20; i++) {
            if(future.isDone())
                break;
            Util.sleep(500);
        }

        Throwable ex=handler.getException();
        assert ex != null && ex instanceof NullPointerException;
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
