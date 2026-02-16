package org.jgroups.raft.internal.registry;

import java.lang.reflect.InvocationTargetException;

/**
 * Wraps a method invocation to be executed in the {@link org.jgroups.raft.StateMachine}.
 *
 * @since 2.0
 * @author José Bolina
 */
@FunctionalInterface
public interface ReplicatedMethodWrapper {

    /**
     * Invokes the method in the {@link org.jgroups.raft.StateMachine} annotated with {@link org.jgroups.raft.StateMachineRead}
     * or {@link org.jgroups.raft.StateMachineWrite}.
     *
     * <p>
     * // The method is invoked in the state machine.
     * // The return value is verified against the expected output type.
     * </p>
     *
     * @param input The input of the state machine method.
     * @return The output of the method execution.
     * @throws InvocationTargetException If is failed to invoke the method through reflection.
     * @throws IllegalAccessException If the method is not accessible.
     * @param <O> The output type of the method.
     */
    <O> O submit(Object ... input) throws InvocationTargetException, IllegalAccessException;
}
