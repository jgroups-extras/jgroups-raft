package org.jgroups.raft;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a write operation in the Raft state machine.
 *
 * <p>
 * Write operations modify the state machine and must go through the Raft consensus protocol to ensure consistency across
 * all nodes in the cluster. These operations are replicated to all nodes and applied in the same order.
 * </p>
 *
 * <p>
 * Each write operation must be uniquely identifiable within the state machine definition by its {@code id} and {@code version}
 * combination. The same id/version pair cannot be shared between write operations.
 * </p>
 *
 * <p>
 * The {@code version} parameter enables backwards compatibility when evolving the state machine interface over time.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaftStateMachine
 * @see StateMachineRead
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface StateMachineWrite {

    /**
     * Unique identifier for this write operation within the state machine definition.
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
