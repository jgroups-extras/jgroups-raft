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
public @interface JGroupsRaftStateMachine { }
