package org.jgroups.raft.internal.serialization;

import org.jgroups.raft.internal.registry.SerializationRegistry;

/**
 * Defines the core contract for object serialization and deserialization within the JGroups Raft library.
 *
 * <p>
 * This abstraction isolates the Raft replication mechanics from the underlying serialization framework. It is utilized
 * to convert user commands, state machine payloads, and cluster snapshots into byte arrays suitable for network transmission
 * and log persistence.
 * </p>
 *
 * <p>
 * The internal Raft commands utilized in the protocol (AppendEntries, LeaderElection, etc.) do not utilize ProtoStream.
 * This interface handles only the API and user-facing operations.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see org.jgroups.raft.JGroupsRaftCustomMarshaller
 */
public interface Serializer {

    /**
     * Serializes the provided object into a byte array.
     *
     * @param object The object to serialize.
     * @return A byte array representing the serialized object.
     */
    byte[] serialize(Object object);

    /**
     * Deserializes the provided byte array back into an object instance.
     *
     * @param buffer The byte array to deserialize.
     * @param <T>    The expected type of the deserialized object.
     * @return The deserialized object.
     */
    <T> T deserialize(byte[] buffer);

    /**
     * Factory method to create the default {@link Serializer} implementation backed by ProtoStream.
     *
     * @param registry The serialization registry containing user-defined schemas and marshallers.
     * @return A ready-to-use ProtoStream serializer.
     */
    static Serializer protoStream(SerializationRegistry registry) {
        return new ProtoStreamSerializer(registry);
    }
}
