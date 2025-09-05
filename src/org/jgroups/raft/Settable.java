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

    /**
     * Synchronous get operation bounded by a timeout.
     * <p>
     * This method blocks until the change has been committed or a timeout occurred.
     * </p>
     *
     * @param buf The buffer representing the read-only operation to submit to the state machine.
     * @param offset The offset to skip the bytes in the buffer.
     * @param length The number of bytes to use from the buffer starting at offset.
     * @param timeout The operation timeout value.
     * @param unit The unit of the operation timeout value.
     * @return A buffer representing the result after submitting the read-only operation.
     * @throws Exception Thrown if the operation could not be submitted.
     * @see #getAsync(byte[], int, int, Options)
     */
    default byte[] get(byte[] buf, int offset, int length, long timeout, TimeUnit unit) throws Exception {
        CompletableFuture<byte[]> cf = getAsync(buf, offset, length, null);
        return cf.get(timeout, unit);
    }

    default CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length) throws Exception {
        return setAsync(buf, offset, length, null);
    }

    default CompletableFuture<byte[]> getAsync(byte[] buf, int offset, int length) throws Exception {
        return getAsync(buf, offset, length, null);
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

    /**
     * Asynchronous get operation that returns immediately without blocking.
     * <p>
     * This method submits a read-only operation to the state machine. Read-only operations are treated differently by
     * the replication algorithm. Since read-only operations <b>do not</b> change the state-machine state, these operations
     * are not appended to the replicated log.
     * </p>
     *
     * <p>
     * <b>Warning:</b> Do not change the state-machine state by operations submitted through this method. Otherwise, the
     * state-machine will diverge and lead to an undefined state.
     * </p>
     *
     * @param buf The buffer representing the read-only operation to submit to the state machine.
     * @param offset The offset to skip the bytes in the buffer.
     * @param length The number of bytes to use from the buffer starting at offset.
     * @return A buffer representing the result after submitting the read-only operation.
     * @throws Exception Thrown if the operation could not be submitted.
     */
    CompletableFuture<byte[]> getAsync(byte[] buf, int offset, int length, Options options) throws Exception;
}
