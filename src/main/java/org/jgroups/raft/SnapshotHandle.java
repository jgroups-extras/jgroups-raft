package org.jgroups.raft;

import java.io.DataOutput;

/**
 * A frozen, point-in-time view of a state machine's contents, ready for serialization.
 *
 * <p>
 * Obtained from {@link AsyncSnapshot#prepareSnapshot()}. The handle must remain valid and independent of any state machine
 * mutations until {@link #release()} is called.
 * </p>
 *
 * <h3>Lifecycle</h3>
 * <p>
 * The framework guarantees that {@link #release()} is called exactly once, regardless of whether {@link #writeTo(DataOutput)}
 * was invoked, completed, or failed. Implementations must release all held resources in {@link #release()}, database snapshot
 * handles, pinned memory, file descriptors, etc.
 * </p>
 *
 * @since 2.0
 * @see AsyncSnapshot#prepareSnapshot()
 */
public interface SnapshotHandle {

    /**
     * Serializes the frozen state to the output stream.
     *
     * <p>
     * Writes the complete state captured by {@link AsyncSnapshot#prepareSnapshot()} to the provided output stream. The
     * output must be readable by {@link AsyncSnapshot#readContentFrom(java.io.DataInput)}.
     * </p>
     *
     * <p>
     * This method may be called concurrently with {@link StateMachine#apply(byte[], int, int, boolean)}. The implementation
     * must only read from the frozen state captured during {@link AsyncSnapshot#prepareSnapshot()}, never from the live,
     * mutating state. Failing to guarantee this requirement will lead to an undefined state in the snapshot the could
     * violate consistency guarantees.
     * </p>
     *
     * <h3>Cancellation</h3>
     *
     * <p>
     * If the node is shutting down, the calling thread is interrupted. Implementations should either check {@link Thread#isInterrupted()}
     * periodically or let {@link InterruptedException} propagate from blocking I/O.
     * </p>
     *
     * @param out the output stream
     * @throws Exception on serialization failure
     */
    void writeTo(DataOutput out) throws Exception;

    /**
     * Releases resources held by this snapshot handle.
     *
     * <p>
     * Always called exactly once by the framework, regardless of whether {@link #writeTo(DataOutput)} was called, completed
     * successfully, or threw an exception. After this method returns, the handle must not hold any resources.
     * </p>
     */
    void release();
}
