package org.jgroups.raft.internal.registry;

import java.lang.reflect.Type;

/**
 * Represents the input or output schema of a method defined within a state machine.
 *
 * <p>
 * This record encapsulates the reflection {@link Type} of a method's parameter or return value of method with the
 * {@link org.jgroups.raft.internal.command.JRaftWriteCommand} or {@link org.jgroups.raft.internal.command.JRaftReadCommand}
 * annotations.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @param type The underlying {@link Type} of the state machine method's parameter or return value.
 */
record CommandSchema(Type type) { }
