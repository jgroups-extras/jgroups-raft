package org.jgroups.raft;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
        return future.get();
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
        return future.get(timeout, unit);
    }


    default CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length) throws Exception {
        return setAsync(buf, offset, length, null);
    }

    /**
     * Asynchronous set, returns immediately with a CompletableFuture. To wait for the result,
     * {@link CompletableFuture#get()} or {@link CompletableFuture#get(long, TimeUnit)} can be called.
     * @param buf The buffer (usually a serialized command) which represent the change to be applied to all state machines
     * @param offset The offset into the buffer
     * @param length he number of bytes to be used in the buffer, starting at offset
     * @param options Options to pass to the call, may be null
     * @return A CompletableFuture which can be used to fetch the result.
     */
    CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Options options) throws Exception;
}
