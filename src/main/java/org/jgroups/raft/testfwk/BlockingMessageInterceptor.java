package org.jgroups.raft.testfwk;

import org.jgroups.Message;
import org.jgroups.util.CompletableFutures;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * Blocks while handling a message.
 * <p>
 * The interceptor receives a predicate to verify on each message. In case the predicate evaluates to <code>true</code>,
 * the caller decides whether to block the message. This mechanism is useful to create breakpoints at specific points
 * of an algorithm or message exchange. This utilization creates more contrived scenarios, or manually creates specific
 * sequence of events.
 * </p>
 *
 * <p>
 * Blocking with the interceptor adds the message to a queue and blocks until another thread intervenes and releases
 * the message. In case of synchronous execution with the {@link MockRaftCluster}, the method blocks the invoking thread.
 * In cases the cluster is asynchronous, the message is delayed until delivered. The thread remains blocked or the message
 * is delayed until it is released by another thread. However, we include a hardcoded 60 seconds timeout to avoid threads
 * leaking during the test execution.
 * </p>
 *
 * <p>
 * Additionally, the interceptor include some helper assertion methods, to validate the queued operations.
 * </p>
 *
 * @since 1.0.13
 * @author José Bolina
 */
@ThreadSafe
public final class BlockingMessageInterceptor {

    private final Predicate<Message> predicate;

    @GuardedBy("this")
    private final Queue<Waiter> waiters;

    public BlockingMessageInterceptor(Predicate<Message> predicate) {
        this.predicate = predicate;
        this.waiters = new ArrayDeque<>();
    }

    /**
     * Check if the given message should be blocked.
     *
     * @param message Message to check the headers.
     * @return <code>true</code> if the message blocks. <code>false</code>, otherwise.
     */
    public boolean shouldBlock(Message message) {
        return predicate.test(message);
    }

    /**
     * Blocks or delay the sending of the given message.
     *
     * <p>
     * Invoking this method will <b>block</b> the invoking thread if operating synchronously. The asynchronous execution
     * simulates a delay in the message delivery and does not block the invoking thread.
     * </p>
     *
     * @param message Message identified to block.
     * @param async Whether to block the thread or introduce an asynchronous delay.
     * @param onComplete Block to run after the asynchronous delay finishes. Must be non-null in case
     *                    {@param async} is <code>true</code>.
     * @throws RuntimeException An unchecked exception in case the thread is interrupted.
     * @throws AssertionError If the execution is async and there is no runnable to run on completion.
     */
    public void blockMessage(Message message, boolean async, Runnable onComplete) {
        assert !async || onComplete != null : "Async operations need to pass runnable on complete";
        Waiter waiter = new Waiter(message, async);
        synchronized (this) {
            waiters.offer(waiter);
        }

        CompletableFuture<Void> cf = waiter.block();
        if (async) cf.thenRun(onComplete);
    }

    /**
     * Blocks the invoking thread while sending the given message.
     *
     * <p>
     * Invoking this method will <b>block</b> the invoking thread.
     * </p>
     *
     * @param message Message identified to block.
     * @see #blockMessage(Message, boolean, Runnable)
     */
    public void blockMessage(Message message) {
        blockMessage(message, false, null);
    }

    /**
     * Releases the next blocked message in line.
     *
     * @throws IllegalStateException In case there is no blocked message.
     */
    public void releaseNext() {
        Waiter waiter;
        synchronized (this) {
            waiter = waiters.poll();
        }

        if (waiter == null)
            throw new IllegalStateException("No blocked messages");

        waiter.done();
    }

    /**
     * Helper to assert an expected number of blocked messages.
     *
     * @param size Expected number of blocked messages.
     * @throws AssertionError In case the actual number of blocked messages is different.
     */
    public void assertNumberOfBlockedMessages(int size) {
        int s = numberOfBlockedMessages();
        assert s == size : String.format("Expected %d waiters, found %d", size, s);
    }

    /**
     * Helper to assert there is no more blocked messages.
     *
     * @throws AssertionError In case there is still blocked messages.
     */
    public void assertNoBlockedMessages() {
        assertNumberOfBlockedMessages(0);
    }

    /**
     * The number of blocked messages.
     *
     * @return The number of blocked messages.
     */
    public synchronized int numberOfBlockedMessages() {
        return waiters.size();
    }

    private static class Waiter {
        private final Message message;
        private final CompletableFuture<Void> cf;
        private final boolean async;

        private Waiter(Message message, boolean async) {
            this.message = message;
            this.cf = new CompletableFuture<>();
            this.async = async;
        }

        private void done() {
            cf.complete(null);
        }

        private CompletableFuture<Void> block() {
            try {
                if (async) return cf.orTimeout(60, TimeUnit.SECONDS);

                cf.get(60, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(String.format("Operation never released: %s", message), e);
            } catch (ExecutionException ignore) { }

            return CompletableFutures.completedNull();
        }
    }
}
