package org.jgroups.raft;

import java.io.DataInput;

/**
 * Optional interface for state machines that support non-blocking snapshot creation.
 *
 * <p>
 * A {@link StateMachine} that also implements this interface opts into a two-phase snapshot lifecycle: a fast capture
 * phase ({@link #prepareSnapshot()}) that freezes a consistent view of the state, followed by a slow serialization phase
 * ({@link SnapshotHandle#writeTo(java.io.DataOutput)}) that runs without blocking new commits from being applied.
 * </p>
 *
 * <p>
 * State machines that do not implement this interface continue to use the synchronous
 * {@link StateMachine#writeContentTo(java.io.DataOutput)} path, which blocks commits until serialization completes.
 * </p>
 *
 * <h3>Consistency Contract</h3>
 * <p>
 * The implementation is solely responsible for ensuring the {@link SnapshotHandle} returned by {@link #prepareSnapshot()}
 * is immune to concurrent state mutations. The framework does <b>NOT</b> provide any isolation. It resumes applying commits
 * immediately after {@code prepareSnapshot()} returns.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SnapshotHandle
 * @see StateMachine
 */
public interface AsyncSnapshot {

    /**
     * Captures a point-in-time consistent view of the state machine.
     *
     * <p>
     * The returned {@link SnapshotHandle} represents a frozen, immutable view of the state machine at the moment this
     * method returns. The state machine may continue to receive {@link StateMachine#apply(byte[], int, int, boolean)}
     * calls after this method returns. The returned handle must not be affected by subsequent mutations. This guarantee
     * is up to the implementer, not complying leads to the creation of snapshot with an undefined state.
     * </p>
     *
     * <p>
     * This method must return quickly. Expensive work, such as serialization, I/O, compression, belongs in
     * {@link SnapshotHandle#writeTo(java.io.DataOutput)}, not here. This method serves as only a mechanism to prepare
     * the underlying state machine for a read of a point in time.
     * </p>
     *
     * <h3>Consistency Guarantee</h3>
     *
     * <p>
     * The implementation is responsible for ensuring the returned handle is immune to concurrent mutations. Typical strategies
     * include capturing a reference to a copy-on-write structure, deep-copying the state, or obtaining a database snapshot
     * handle (e.g., RocksDB, LMDB). Observe that performing a deep-copy of a very large object <b>is</b> an expensive
     * operation that could stall progress.
     * </p>
     *
     * <h3>Failures</h3>
     *
     * <p>
     * If this method throws, no snapshot is created and {@link SnapshotHandle#release()} is not called. The framework
     * retries on the next snapshot threshold. The lack of snapshot could render some nodes effectively unavailable since
     * they can't commit newer requests.
     * </p>
     *
     * @return a handle to the frozen state, never {@code null}
     * @throws Exception if the snapshot cannot be prepared
     */
    SnapshotHandle prepareSnapshot() throws Exception;

    /**
     * Restores the state machine from a snapshot stream.
     *
     * <p>
     * Replaces the entire state machine contents with the state serialized by a prior
     * {@link SnapshotHandle#writeTo(java.io.DataOutput)} call. The implementation must completely read the stream and
     * restore its internal state deterministically before returning. The implementation may need to clear all existing
     * state before populating from the stream.
     * </p>
     *
     * <p>
     * This method is never called concurrently with {@link StateMachine#apply(byte[], int, int, boolean)}. No locking
     * mechanism is needed.
     * </p>
     *
     * <h3>Failures</h3>
     *
     * <p>
     * If an exception is thrown, the state machine may be left in an inconsistent state. Implementations should ensure
     * restoration is atomic or clearly document partial-failure behavior. The underlying mechanism will not retry or
     * perform any cleanup login in case of exceptions. The Raft implementation assumes the recovery mechanism leaves a
     * perfect state machine after restoration.
     * </p>
     *
     * @param in the input stream containing the serialized snapshot
     */
    void readContentFrom(DataInput in);
}
