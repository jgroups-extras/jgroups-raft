package org.jgroups.raft.testfwk;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Base class for the cluster implementations in the test framework.
 *
 * <p>
 * The cluster abstraction facilitates the creation of a cluster during the tests. This approach avoids the need to
 * create actual {@link org.jgroups.JChannel} and complex configurations. This abstraction provides a simplified and
 * controllable way to test the cluster, emitting events and sending messages.
 * </p>
 *
 * <p>
 * The cluster abstraction works through {@link View} updates. Members are added and removed manually. When a new view
 * is received, the members receive the update accordingly and follow the synchronous configuration. The user utilizing
 * this abstraction has more control over when a member joins and which messages it receives.
 * </p>
 *
 * Our test suite contains examples of uses of the cluster abstraction.
 *
 * @since 1.0.12
 */
public abstract class MockRaftCluster {

    protected final Executor thread_pool=createThreadPool(1000);
    protected boolean        async;
    protected BlockingMessageInterceptor interceptor = null;

    /**
     * Emit the view update to all cluster members.
     * <p>
     * How the view is updated might vary from implementation. The basic idea is to iterate over all members and
     * invoke each protocol in the stack to handle the view.
     * </p>
     *
     * @param view: The new {@link View} instance to update the member.
     */
    public abstract void handleView(View view);

    /**
     * Send a message in the cluster.
     *
     * @param msg: Message to send.
     */
    public abstract void send(Message msg);

    /**
     * The number of member in the cluster.
     *
     * @return The size of the cluster.
     */
    public abstract int size();

    /**
     * Add a new member to the cluster.
     *
     * <p>
     * The new member is associated with the given address. A member is resolved by the address when sending a message.
     * Also, note that a view update is necessary to propagate the member addition.
     * </p>
     *
     * @param addr: The new member's address.
     * @param node: The member abstraction wrapped in {@link RaftNode}.
     * @return The fluent current class.
     * @param <T>: The current instance type.
     */
    public abstract <T extends MockRaftCluster> T add(Address addr, RaftNode node);

    /**
     * Remove a member from the cluster.
     * <p>
     * A view update is necessary to propagate the member removal.
     * </p>
     *
     * @param addr: The address of the member to remove.
     * @return The fluent current class.
     * @param <T>: The current instance type.
     */
    public abstract <T extends MockRaftCluster> T remove(Address addr);

    /**
     * Remove all members from the cluster.
     *
     * @return The fluent current class.
     * @param <T>: The current instance type.
     */
    public abstract <T extends MockRaftCluster> T clear();

    /**
     * Intercept messages before sending.
     * <p>
     * Before sending each message, the interceptor verifies whether to block. The messages must be released utilizing
     * the returned instance.
     * </p>
     *
     * <b>Warning:</b> Blocking a message in a synchronous cluster will block the calling thread.
     *
     * <p>
     * To simulate a delay in the message, utilize the {@link #async(boolean)} method passing <code>true</code>. This
     * will not block the invoking thread.
     * </p>
     *
     * @param predicate: The predicate to check whether to block.
     * @return A new {@link BlockingMessageInterceptor} instance to control the blocking mechanism.
     */
    public BlockingMessageInterceptor addCommandInterceptor(Predicate<Message> predicate) {
        return this.interceptor = new BlockingMessageInterceptor(predicate);
    }

    /**
     * Utility to create a fluent use.
     *
     * @return The fluent current class.
     * @param <T>: The current instance type.
     */
    @SuppressWarnings("unchecked")
    protected final <T extends MockRaftCluster> T self() {
        return (T) this;
    }

    /**
     * Check whether the cluster is in asynchronous mode.
     *
     * @return <code>true</code> if asynchronous, <code>false</code>, otherwise.
     */
    public boolean     async()                            {return async;}

    /**
     * Update the cluster mode between synchronous and asynchronous.
     *
     * @param b: <code>true</code> to run asynchronous, <code>false</code> to run synchronous.
     * @return The fluent current class.
     * @param <T>: The current instance type.
     */
    public <T extends MockRaftCluster> T async(boolean b) {
        async=b;
        return self();
    }

    /**
     * Create the {@link Executor} to submit tasks during asynchronous mode.
     *
     * @param max_idle_ms: Executor configuration parameter.
     * @return The {@link Executor} instance to utilize in the cluster abstraction.
     */
    protected Executor createThreadPool(long max_idle_ms) {
        int max_cores = Math.max(Runtime.getRuntime().availableProcessors(), 4);
        return new ThreadPoolExecutor(0, max_cores, max_idle_ms, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>());
    }

    /**
     * Asynchronously sends a message up the node stack.
     *
     * @param node: The node to handle the message.
     * @param msg: The message to send.
     */
    protected void deliverAsync(RaftNode node, Message msg) {
        thread_pool.execute(() -> node.up(msg));
    }
}
