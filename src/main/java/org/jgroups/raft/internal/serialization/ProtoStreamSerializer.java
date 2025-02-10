package org.jgroups.raft.internal.serialization;

import org.jgroups.raft.internal.registry.SerializationRegistry;

import java.io.IOException;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.types.java.CommonContainerTypesSchema;
import org.infinispan.protostream.types.java.CommonTypesSchema;

class ProtoStreamSerializer implements Serializer {
    private static final byte[] EMPTY = {};
    private final SerializationRegistry registry;

    public ProtoStreamSerializer(SerializationRegistry registry) {
        this.registry = registry;
        registerInternalSerializers(this, registry);
    }

    private static void registerInternalSerializers(Serializer serializer, SerializationRegistry registry) {
        registry.register(new CommonTypesSchema());
        registry.register(new CommonContainerTypesSchema());
        registry.register(new RaftSerializationInitializerImpl());

        registry.register(new ObjectWrapper.Marshaller(RaftSerializationInitializer.getFullTypeName(ObjectWrapper.class), serializer));
    }

    @Override
    public byte[] serialize(Object object) {
        if (object == null)
            return EMPTY;

        if (!isSerializable(object)) {
            object = ObjectWrapper.create(object);
        }

        try {
            return ProtobufUtil.toWrappedByteArray(registry.context(), object);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed serializing object", e);
        }
    }

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
