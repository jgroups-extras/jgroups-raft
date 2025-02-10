package org.jgroups.raft.internal.serialization;

import org.jgroups.raft.internal.registry.SerializationRegistry;

public interface Serializer {

    byte[] serialize(Object object);

    <T> T deserialize(byte[] buffer);

    static Serializer protoStream(SerializationRegistry registry) {
        return new ProtoStreamSerializer(registry);
    }
}
