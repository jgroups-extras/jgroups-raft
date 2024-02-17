package org.jgroups.raft.testfwk;

import org.jgroups.Header;
import org.jgroups.Message;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/**
 * Blocks while handling a message.
 * <p>
 * The interceptor receives a predicate to verify on the headers of each message. In case the predicate evaluates to
 * <code>true</code>, the caller decides whether to block the message. This mechanism is useful to create breakpoints
 * at specific points of an algorithm or message exchange. This utilization creates more contrived scenarios, or
 * manually creates specific sequence of events.
 * </p>
 *
 * <p>
 * Blocking with the interceptor adds the message to a queue and blocks until another thread intervenes and releases
 * the message. The thread remains blocked until the release. However, we include a hardcoded 60 seconds timeout to
 * avoid threads leaking during the test execution.
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

    private final Predicate<Header> predicate;

    @GuardedBy("this")
    private final Queue<Waiter> waiters;


    public BlockingMessageInterceptor(Predicate<Header> predicate) {
        this.predicate = predicate;
        this.waiters = new ArrayDeque<>();
    }

    /**
     * Check if the given message should be blocked.
     *
     * @param message: Message to check the headers.
     * @return <code>true</code> if the message blocks. <code>false</code>, otherwise.
     */
    public boolean shouldBlock(Message message) {
        for (Map.Entry<Short, Header> entry : message.getHeaders().entrySet()) {
            if (predicate.test(entry.getValue())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Blocks the invoking thread while sending the given message.
     *
     * <p>
     * Invoking this method will <b>block</b> the invoking thread.
     * </p>
     *
     * @param message: Message identified to block.
     * @throws RuntimeException: An unchecked exception in case the thread is interrupted.
     */
    public void blockMessage(Message message) {
        Waiter waiter = new Waiter(message);
        synchronized (this) {
            waiters.offer(waiter);
        }

        waiter.block();
    }

    /**
     * Releases the next blocked message in line.
     *
     * @throws IllegalStateException: In case there is no blocked message.
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
     * @param size: Expected number of blocked messages.
     * @throws AssertionError: In case the actual number of blocked messages is different.
     */
    public void assertNumberOfBlockedMessages(int size) {
        int s = numberOfBlockedMessages();
        assert s == size : String.format("Expected %d waiters, found %d", size, s);
    }

    /**
     * Helper to assert there is no more blocked messages.
     *
     * @throws AssertionError: In case there is still blocked messages.
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
        private final CountDownLatch latch;

        private Waiter(Message message) {
            this.message = message;
            this.latch = new CountDownLatch(1);
        }

        private void done() {
            latch.countDown();
        }

        private void block() {
            try {
                if (!latch.await(60, TimeUnit.SECONDS))
                    throw new IllegalStateException(String.format("Waiter for message %s timed out", message));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
