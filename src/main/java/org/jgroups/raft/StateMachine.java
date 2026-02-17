package org.jgroups.raft;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Interface of a state machine which stores data in memory. Committed log entries are applied to the state machine.
 *
 * <h3>Lifecycle in the Raft Protocol</h3>
 *
 * <p>
 * In the context of the Raft consensus algorithm, the state machine is invoked only after a log entry has achieved consensus
 * (i.e., it has been successfully replicated and committed by a majority of the cluster nodes). It represents the final,
 * durable application of a client's command to the local node.
 * </p>
 *
 * <h3>Strict Determinism</h3>
 *
 * <p>
 * It is absolutely critical that the state machine implementation is strictly deterministic. Given the same initial state
 * and the exact same sequence of applied log entries, every node in the cluster must arrive at the exact same final state.
 * Implementations must <b>never</b> rely on outside factors during execution, such as {@code System.currentTimeMillis()},
 * random number generators, thread scheduling behavior, or external network/database calls. All these non-deterministic
 * behavior could undo the Raft replication work.
 * </p>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>
 * Internally, the Raft protocol guarantees that commands are passed to this interface sequentially by a single event-loop thread.
 * Because it is invoked by a single thread, the {@link #apply} method is inherently thread-safe from the perspective of log
 * replication. You do not need to implement any locking mechanism.
 * </p>
 *
 * <h3>Failure Handling</h3>
 *
 * <p>
 * If the {@link #apply} method throws an exception, the underlying Raft log entry remains committed. Unhandled exceptions
 * during application may cause the node's state to diverge from the cluster, halt the state machine entirely, or leave the
 * system in an undefined state. Implementations should catch and handle business-logic exceptions internally and return a
 * serialized error payload to the client rather than throwing runtime exceptions.
 * </p>
 *
 * @author Bela Ban
 * @since 0.1
 */
public interface StateMachine {

    /**
     * Applies a command to the state machine.
     *
     * <p>
     * The contents of the byte[] buffer are interpreted by the state machine and serialized into the actual command.
     * The serialization mechanism should be strict with the given intervals to avoid copying the byte buffer.
     * </p>
     *
     * <p>
     * This method should never throw an exception as it could leave the protocol in an undefined state. Instead, it should
     * serialize any exception and return the buffer for the client application to handle the exception.
     * </p>
     *
     * @param data               The byte[] buffer
     * @param offset             The offset at which the data starts
     * @param length             The length of the data
     * @param serialize_response If true, serialize and return the response, else return null
     * @return A serialized response value, or null (e.g. if the method returned void)
     */
    byte[] apply(byte[] data, int offset, int length, boolean serialize_response);

    /**
     * Reads the contents of the state machine from an input stream.
     *
     * <p>
     * This can be the case when an InstallSnapshot RPC is used to bootstrap a new node, or a node that's lagging far behind.
     * The parsing depends on the concrete state machine implementation, but the idea is that the stream is a sequence of commands,
     * each of which can be passed to {@link #apply(byte[], int, int, boolean)}.
     * </p>
     *
     * <h3>Synchronous Execution & Event Loop Blocking</h3>
     * <p>
     * This method is invoked <b>synchronously</b> by the main Raft event loop. While this method is executing, the core Raft
     * protocol is entirely blocked. The node will not process incoming client requests until the restoration is fully complete.
     * </p>
     *
     * <h3>Buffer Ownership & Determinism</h3>
     * <p>
     * The framework owns the provided {@link DataInput} buffer, and this ownership is released the moment this method finishes.
     * The implementation must <b>completely read the buffer and restore its internal state deterministically</b> before
     * returning. The state machine implementation may need to remove all contents before populating itself from the stream.
     * </p>
     *
     * <p>
     * This method is never called concurrently with invocations to {@link #apply(byte[], int, int, boolean)}. No locking
     * mechanism is needed in the state machine.
     * </p>
     *
     * <h3>Failures</h3>
     * <p>
     * Similar to {@link #apply(byte[], int, int, boolean)}, exceptions must be handled carefully when restoring the state
     * machine. If an exception is thrown, the underlying system could be left in an undefined state. You should ensure
     * the state machine remains consistent and deterministic.
     * </p>
     *
     * @param in The input stream
     */
    void readContentFrom(DataInput in);

    /**
     * Writes the contents of the state machine to an output stream.
     *
     * <p>
     * This is typically called on the leader to provide state to a new node, or a node that's lagging far behind. All the
     * data needed to completely recover the state machine should be written to the buffer.
     * </p>
     *
     * <h3>Synchronous Execution & Event Loop Blocking</h3>
     * <p>
     * This method is invoked <b>synchronously</b> by the main Raft event loop. While this method is executing, the core Raft
     * protocol is entirely blocked. Updates to the state machine may need to be put on hold while the state is written to the
     * output stream. Otherwise, it is not possible to guarantee a consistent view of the state machine.
     * </p>
     *
     * <h3>Buffer Ownership & Determinism</h3>
     * <p>
     * The framework owns the {@link DataOutput} stream. The implementation must deterministically and <b>completely write
     * its current state</b> to the stream before returning, as the buffer is flushed and released immediately after the method
     * finishes. Try to stick to the minimum data needed to restore the state machine.
     * </p>
     *
     * <h3>Optimization Recommendations</h3>
     * <p>
     * Because this method blocks the main event loop until the snapshot is fully written, users are strongly encouraged to
     * think carefully about optimizations. To minimize the blocking time, consider maintaining a fast serialization format or
     * utilizing internal Copy-on-Write (COW) data structures so that the actual state iteration and serialization can be
     * performed as quickly as possible.
     * </p>
     *
     * <h3>Failures</h3>
     * <p>
     * If an exception is thrown while taking the snapshot, we do not submit the underlying buffer to remote nodes.
     * </p>
     *
     * @param out The output stream
     * @throws Exception Thrown on serialization or other failure.
     */
    void writeContentTo(DataOutput out) throws Exception;
}
