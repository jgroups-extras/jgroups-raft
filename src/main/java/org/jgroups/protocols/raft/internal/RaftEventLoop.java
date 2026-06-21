package org.jgroups.protocols.raft.internal;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Abstraction over the RAFT single-threaded event loop and its associated background executor.
 *
 * <p>
 * Provides two capabilities: submitting work to the event loop for sequential execution, and obtaining an executor for
 * offloading blocking or long-running tasks outside the event loop. All callables submitted via {@link #submit(Callable)}
 * execute on the event loop thread, preserving the single-writer invariant that RAFT implementation relies on for correctness.
 * </p>
 *
 * <p>
 * <b>Warning:</b> This is an internal implementation detail and not meant as a public interface.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public interface RaftEventLoop {

    /**
     * Submits a callable for execution on the RAFT event loop.
     *
     * <p>
     * The callable runs on the event loop thread, so it may safely access RAFT-internal state without
     * synchronization. The returned stage completes when the callable finishes; it completes exceptionally
     * if the callable throws.
     * </p>
     *
     * @param callable the task to execute on the event loop
     * @param <T> the result type
     * @return a completion stage that resolves with the callable's result
     */
    <T> CompletionStage<T> submit(Callable<T> callable);

    /**
     * Returns an executor for running tasks outside the event loop.
     *
     * <p>
     * Work submitted to this executor runs on a background thread, suitable for blocking operations such as snapshot
     * serialization. The executor's lifecycle is managed by the RAFT protocol.
     * </p>
     *
     * @return the background executor
     */
    Executor executor();
}
