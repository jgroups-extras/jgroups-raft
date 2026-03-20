package org.jgroups.raft;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a Raft state machine.
 *
 * <p>
 * This annotation is required to identify the state machine implementation that will process committed log entries in
 * the Raft cluster. JGroups Raft uses this marker internally when initializing the Raft instance. The class with this
 * annotation is provided to {@link JGroupsRaft#builder(Object, Class)} during the instance creation. Only one class per
 * Raft instance should be annotated with this marker.
 * </p>
 *
 * <h2>State Machine Requirements</h2>
 * <p>
 * To function correctly in a distributed consensus algorithm, the state machine must satisfy:
 * <ul>
 * <li><b>Determinism:</b> Given the same sequence of operations, all replicas must produce identical results and reach the same state.
 *      The implementation should not utilize non-deterministic mechanisms. It should always be deterministic.</li>
 * <li><b>Sequential consistency:</b> Operations must be applied in the exact order they appear in the committed log.</li>
 * <li><b>No side effects:</b> Operations should not perform external I/O, network calls, or other non-deterministic
 *      actions that could cause replica divergence.</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see StateMachineRead
 * @see StateMachineWrite
 * @see StateMachineField
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface JGroupsRaftStateMachine {

    /**
     * Declares read operations that have been permanently removed from this state machine.
     *
     * <p>
     * When a read operation is removed from the state machine interface, existing Raft logs may still contain entries
     * referencing that operation's {@code (id, version)} pair. Declaring the removed operation here tells the schema
     * validator that the removal is intentional, preventing a startup failure.
     * </p>
     *
     * <p>
     * A retired operation cannot be reused — its {@code (id, version)} pair is permanently reserved. Only retire an
     * operation after ensuring no uncompacted log entries reference it. You can utilize the
     * {@link JGroupsRaftAdministration#snapshot()} to ensure entries in the log are compacted.
     * </p>
     *
     * <h2>Example</h2>
     * <pre>{@code
     * // The read operation (id=2, version=1) was removed from the interface.
     * // Declare it as retired to allow the node to start without validation errors.
     * @JGroupsRaftStateMachine(
     *     retiredReads = @StateMachineRead(id = 2, version = 1)
     * )
     * public interface MyStateMachine {
     *     @StateMachineWrite(id = 1, version = 1)
     *     void put(String key, String value);
     * }
     * }</pre>
     *
     * @return the retired read operations, empty by default
     * @see StateMachineRead
     */
    StateMachineRead[] retiredReads() default {};

    /**
     * Declares write operations that have been permanently removed from this state machine.
     *
     * <p>
     * When a write operation is removed from the state machine interface, existing Raft logs may still contain entries
     * referencing that operation's {@code (id, version)} pair. Declaring the removed operation here tells the schema
     * validator that the removal is intentional, preventing a startup failure.
     * </p>
     *
     * <p>
     * A retired operation cannot be reused — its {@code (id, version)} pair is permanently reserved. Only retire an
     * operation after ensuring no uncompacted log entries reference it. You can utilize the
     * {@link JGroupsRaftAdministration#snapshot()} to ensure entries in the log are compacted.
     * </p>
     *
     * <h2>Example</h2>
     * <pre>{@code
     * // The write operation (id=1, version=1) was removed from the interface.
     * // Declare it as retired to allow the node to start without validation errors.
     * @JGroupsRaftStateMachine(
     *     retiredWrites = @StateMachineWrite(id = 1, version = 1)
     * )
     * public interface MyStateMachine {
     *     @StateMachineWrite(id = 2, version = 2)
     *     void put(String key, String value, int ttl);
     * }
     * }</pre>
     *
     * @return the retired write operations, empty by default
     * @see StateMachineWrite
     */
    StateMachineWrite[] retiredWrites() default {};
}
