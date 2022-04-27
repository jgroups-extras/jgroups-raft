package org.jgroups.protocols.raft;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * Interface to make changes to the Raft state machine. All changes are made through the leader, which appends the change
 * to its log and then sends it to all followers. When the majority has acked the change, it will be committed to the log.
 * @author Bela Ban
 * @since  0.1
 */
public interface Settable {
    /**
     * Synchronous set. Blocks until the change has been committed.
     * @param buf The buffer (usually a serialized command) which represent the change to be applied to all state machines
     * @param offset The offset into the buffer
     * @param length The number of bytes to be used in the buffer, starting at offset
     * @return Another buffer, representing the result of applying the change. E.g. for a put(k,v), this might be the
     * serialized result of the previous key in a hashmap
     * @throws Exception Thrown if the change could not be applied/committed, e.g. because there was no majority, or no elected leader
     */
    default byte[] set(byte[] buf, int offset, int length) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length);
        return progressiveWait(future);
    }

    /**
     * Synchronous set bounded by a timeout. Blocks until the change has been committed or a timeout occurred
     * @param buf The buffer (usually a serialized command) which represent the change to be applied to all state machines
     * @param offset The offset into the buffer
     * @param length The number of bytes to be used in the buffer, starting at offset
     * @param timeout The timeout, in unit (below)
     * @param unit The unit of the timeout
     * @return Another buffer, representing the result of applying the change. E.g. for a put(k,v), this might be the
     * serialized result of the previous key in a hashmap
     * @throws Exception Thrown if the change could not be applied/committed, e.g. because there was no majority, or no elected leader
     */
    default byte[] set(byte[] buf, int offset, int length, long timeout, TimeUnit unit) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length);
        return progressiveWait(future, timeout, unit);
    }

    int MAX_SPINS = (Runtime.getRuntime().availableProcessors() < 2) ? 0 : 32;

    static byte[] progressiveWait(CompletableFuture<byte[]> future) throws Exception {
        byte[] result = future.getNow(null);
        if (result != null) {
            return result;
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
        int maxSpins = MAX_SPINS;
        int maxYield = MAX_SPINS == 0 ? 0 : maxSpins + 20;
        int maxTinyPark = MAX_SPINS == 0 ? 0 : maxYield + 900;
        int idleCount = 0;
        while (true) {
            result = future.getNow(null);
            if (result != null) {
                return result;
            }
            if (idleCount < maxSpins) {
                idleCount++;
                Thread.onSpinWait();
            } else if (idleCount < maxYield) {
                idleCount++;
                Thread.yield();
            } else if (idleCount < maxTinyPark) {
                idleCount++;
                LockSupport.parkNanos(1);
            } else {
                // just sit
                return future.get();
            }
        }
    }

    static byte[] progressiveWait(CompletableFuture<byte[]> future, long timeout, TimeUnit unit) throws Exception {
        byte[] result = future.getNow(null);
        if (result != null) {
            return result;
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
        final long startTime = System.nanoTime();
        final long timeoutNanos = unit.toNanos(timeout);
        int maxSpins = MAX_SPINS;
        int maxYield = MAX_SPINS == 0 ? 0 : maxSpins + 20;
        int maxTinyPark = MAX_SPINS == 0 ? 0 : maxYield + 900;
        int idleCount = 0;
        while (true) {
            final long elapsed = System.nanoTime() - startTime;
            final long remaining = timeoutNanos - elapsed;
            if (remaining <= 0) {
                throw new TimeoutException();
            }
            result = future.getNow(null);
            if (result != null) {
                return result;
            }
            if (idleCount < maxSpins) {
                idleCount++;
                Thread.onSpinWait();
            } else if (idleCount < maxYield) {
                idleCount++;
                Thread.yield();
            } else if (idleCount < maxTinyPark) {
                idleCount++;
                LockSupport.parkNanos(1);
            } else {
                // just sit
                return future.get(remaining, TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * Asynchronous set, returns immediately with a CompletableFuture. To wait for the result,
     * {@link CompletableFuture#get()} or {@link CompletableFuture#get(long, TimeUnit)} can be called.
     * @param buf The buffer (usually a serialized command) which represent the change to be applied to all state machines
     * @param offset The offset into the buffer
     * @param length he number of bytes to be used in the buffer, starting at offset
     * @return A CompletableFuture which can be used to fetch the result.
     */
    CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length) throws Exception;
}
