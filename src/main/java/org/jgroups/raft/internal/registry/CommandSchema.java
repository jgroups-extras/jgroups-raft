package org.jgroups.raft.internal.registry;

import org.jgroups.raft.util.ClassUtil;

import java.lang.reflect.Type;

/**
 * Represents the input or output schema of a method defined within a state machine.
 *
 * <p>
 * This record encapsulates the reflection {@link Type} of a method's parameter or return value of method with the
 * {@link org.jgroups.raft.internal.command.JRaftWriteCommand} or {@link org.jgroups.raft.internal.command.JRaftReadCommand}
 * annotations. It provides validation mechanisms utilized during system restart to verify compatibility between the persisted
 * schema and the current Java definitions.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @param type The underlying {@link Type} of the state machine method's parameter or return value.
 */
record CommandSchema(Type type) {

    /**
     * Verifies if the provided class is an exact type match to this schema.
     *
     * <p>
     * This method strictly checks for type equivalence rather than assignability. This is utilized after a system restart
     * to validate that the state machine schema stored on disk perfectly matches the current Java class, verifying
     * backwards compatibility.
     * </p>
     *
     * @param other The class to compare against this schema's type.
     * @return {@code true} if the types are an exact match, {@code false} otherwise.
     */
    public boolean isTypeExactMatch(Class<?> other) {
        return ClassUtil.isEquivalent(other, type);
    }
}
