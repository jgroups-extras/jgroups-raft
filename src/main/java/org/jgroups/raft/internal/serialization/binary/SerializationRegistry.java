package org.jgroups.raft.internal.serialization.binary;

import org.jgroups.raft.serialization.JGroupsRaftCustomMarshaller;

import java.util.Objects;

/**
 * Registry for user-provided custom serializers.
 *
 * <p>
 * This class collects user-facing {@link JGroupsRaftCustomMarshaller} implementations and registers them with the internal
 * binary serialization system. It serves as the public API entry point for serializer registration.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */

public sealed interface SerializationRegistry permits SerializationRegistry.DefaultSerializationRegistry {

    /**
     * Registers a custom marshaller for a user type.
     *
     * <p>
     * The marshaller's {@link JGroupsRaftCustomMarshaller#type()} must return a unique type ID. Type IDs must be unique across
     * the cluster and should never be reused once assigned.
     * </p>
     *
     * @param marshaller The custom marshaller to register
     * @param <T> The type being marshalled
     * @throws IllegalStateException if a marshaller for the same class or type ID is already registered.
     * If the type ID is smaller than {@link JGroupsRaftCustomMarshaller#MINIMUM_TYPE_ID}.
     */
    <T> void register(JGroupsRaftCustomMarshaller<T> marshaller);

    /**
     * Creates a new serialization registry.
     *
     * @return A new registry instance
     */
    static SerializationRegistry create() {
        return new DefaultSerializationRegistry();
    }

    /**
     * Default implementation of the serialization registry.
     *
     * <p>
     * This is a simple wrapper around {@link BinarySerializationRegistry} that provides
     * the public API for registering user serializers.
     * </p>
     */
    final class DefaultSerializationRegistry implements SerializationRegistry {

        private final BinarySerializationRegistry registry;

        private DefaultSerializationRegistry() {
            this.registry = new BinarySerializationRegistry();
        }

        @Override
        public <T> void register(JGroupsRaftCustomMarshaller<T> marshaller) {
            Objects.requireNonNull(marshaller, "marshaller is null");

            if (marshaller.type() < JGroupsRaftCustomMarshaller.MINIMUM_TYPE_ID) {
                String message = "Marshaller '%s' for class '%s' has a type ID of %d which is below the minimum value of %d. "
                        + "You can utilize %s#MINIMUM_TYPE_ID to order your types";
                throw new IllegalStateException(String.format(message, marshaller.getClass().getName(), marshaller.javaClass().getName(),
                        marshaller.type(), JGroupsRaftCustomMarshaller.class.getName(), JGroupsRaftCustomMarshaller.MINIMUM_TYPE_ID));
            }

            registry.registerSerializer(marshaller);
        }

        BinarySerializationRegistry get() {
            return registry;
        }
    }
}
