package org.jgroups.raft;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a read-only operation in the Raft state machine.
 *
 * <p>
 * Read operations do not modify the state machine and can be executed locally without going through the consensus protocol.
 * This provides better performance for queries that don't require strong consistency. Read-only operations also employ the
 * optimization described in the Raft paper to ensure linearizability by checking the leader's commit index, in case
 * linearizability is required.
 * </p>
 *
 * <p>
 * Each read operation must be uniquely identifiable within the state machine class by its {@code id} and {@code version} combination.
 * The same id/version pair cannot be shared between read operations.
 * </p>
 *
 * <p>
 * The {@code version} parameter enables backwards compatibility when evolving the state machine interface over time.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaftStateMachine
 * @see StateMachineWrite
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface StateMachineRead {

    /**
     * Unique identifier for this read operation within the state machine definition.
     *
     * @return the operation identifier
     */
    long id();

    /**
     * Version number for backwards compatibility.
     *
     * <p>
     * Allows evolution of the state machine interface while maintaining compatibility with existing clients.
     * </p>
     *
     * @return the operation version, defaults to 1
     */
    int version() default 1;
}
