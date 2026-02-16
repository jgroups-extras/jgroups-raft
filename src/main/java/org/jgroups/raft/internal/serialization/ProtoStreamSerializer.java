package org.jgroups.raft.internal.serialization;

import org.jgroups.raft.JGroupsRaftCustomMarshaller;
import org.jgroups.raft.internal.registry.SerializationRegistry;

import java.io.IOException;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.types.java.CommonContainerTypesSchema;
import org.infinispan.protostream.types.java.CommonTypesSchema;

/**
 * The default {@link Serializer} implementation backed by the Infinispan ProtoStream library.
 *
 * <p>
 * This serializer bridges the JGroups Raft ecosystem with ProtoStream for backwards-compatible encoding format. It is
 * responsible for evaluating object serializability, managing internal serialization loops, and unwrapping polymorphically
 * encapsulated objects during reads.
 * </p>
 *
 * <p>
 * The objective with ProtoStream is to ensure backwards-compatible functionality across versions. The users can upgrade
 * their application, and it would still communicate with older versions. The same applies during the internal development,
 * where it is not allowed breaking changes.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
final class ProtoStreamSerializer implements Serializer {
    private static final byte[] EMPTY = {};
    private final SerializationRegistry registry;

    /**
     * Initializes the serializer and registers essential internal schemas.
     *
     * @param registry The registry holding the serialization context.
     */
    ProtoStreamSerializer(SerializationRegistry registry) {
        this.registry = registry;
        registerInternalSerializers(this, registry);
    }

    private static void registerInternalSerializers(Serializer serializer, SerializationRegistry registry) {
        registry.register(new CommonTypesSchema());
        registry.register(new CommonContainerTypesSchema());
        registry.register(new RaftSerializationInitializerImpl());

        registry.register(new ObjectWrapper.Marshaller(RaftSerializationInitializer.getFullTypeName(ObjectWrapper.class), serializer));
    }

    /**
     * Serializes an object to a byte array using ProtoStream.
     *
     * <p>
     * <b>Fail-Fast Validation:</b> Before attempting serialization, this method strictly verifies if the object's class
     * is known to the registry (either natively, via a generated schema, or via a custom marshaller). If the class is
     * unregistered, it immediately throws an {@link IllegalArgumentException} to prevent recursive stack overflows.
     * </p>
     *
     * @param object The object to serialize.
     * @return The serialized byte array, or an empty array if the object is null.
     * @throws IllegalArgumentException If the object is not registered for serialization or an I/O error occurs.
     */
    @Override
    public byte[] serialize(final Object object) {
        if (object == null)
            return EMPTY;

        if (!isSerializable(object)) {
            String message =
                    "Object of class (%s) is not serializable. "
                    + "You should register a customer marshaller implementing (%s) or utilize ProtoStream to generate "
                    + " a schema for your class.";
            throw new IllegalArgumentException(String.format(message, object.getClass().getName(), JGroupsRaftCustomMarshaller.class.getName()));
        }

        try {
            return ProtobufUtil.toWrappedByteArray(registry.context(), object);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed serializing object", e);
        }
    }

    /**
     * Deserializes a byte array back into an object.
     *
     * <p>
     * If the underlying data was packaged inside an {@link ObjectWrapper} (e.g., to support polymorphic fields), this method
     * automatically unwraps it before returning the final object to the caller.
     * </p>
     *
     * @param buffer The byte array to deserialize.
     * @return The deserialized object, or null if the buffer is empty.
     * @throws IllegalArgumentException If an I/O error occurs during deserialization.
     */
    @Override
    public <T> T deserialize(byte[] buffer) {
        if (buffer == null || buffer.length == 0) return null;

        try {
            return unwrap(ProtobufUtil.fromWrappedByteArray(registry.context(), buffer, 0, buffer.length));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize object", e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T unwrap(Object o) {
        if (o instanceof ObjectWrapper<?> ow) {
            return (T) ObjectWrapper.unwrap(ow);
        }
        return (T) o;
    }

    private boolean isSerializable(Object obj) {
        return isSerializableWithoutWrapping(obj) || registry.context().canMarshall(obj.getClass());
    }

    public boolean isSerializableWithoutWrapping(Object o) {
        return o instanceof String ||
                o instanceof Long ||
                o instanceof Integer ||
                o instanceof Double ||
                o instanceof Float ||
                o instanceof Boolean ||
                o instanceof byte[] ||
                o instanceof Byte ||
                o instanceof Short ||
                o instanceof Character ||
                o instanceof java.util.Date ||
                o instanceof java.time.Instant;
    }
}
