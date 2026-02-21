package org.jgroups.raft.internal.serialization;

import org.jgroups.raft.internal.serialization.binary.BinarySerializer;
import org.jgroups.raft.internal.serialization.binary.SerializationRegistry;
import org.jgroups.raft.serialization.JGroupsRaftCustomMarshaller;

import java.io.DataOutput;

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
 * @see JGroupsRaftCustomMarshaller
 */
public interface Serializer {

    /**
     * Serializes the provided object into a byte array.
     *
     * @param object The object to serialize.
     * @return A byte array representing the serialized object.
     * @throws org.jgroups.raft.exceptions.JGroupsRaftSerializationException if an error happens during serialization.
     */
    byte[] serialize(Object object);

    /**
     * Serializes the provided object into the give output stream.
     *
     * @param output The output to write to.
     * @param object The object to serialize.
     * @throws org.jgroups.raft.exceptions.JGroupsRaftSerializationException if an error happens during serialization.
     */
    void serialize(DataOutput output, Object object);

    /**
     * Deserializes the provided byte array back into an object instance.
     *
     * @param buffer The byte array to deserialize.
     * @param clazz The target class to deserialize.
     * @param <T>    The expected type of the deserialized object.
     * @return The deserialized object.
     */
    <T> T deserialize(byte[] buffer, Class<T> clazz);

    /**
     * Factory method to create the default {@link Serializer} implementation backed by ProtoStream.
     *
     * @param registry The serialization registry containing user-defined schemas and marshallers.
     * @return A ready-to-use ProtoStream serializer.
     */
    static Serializer create(SerializationRegistry registry) {
        return new BinarySerializer(registry);
    }
}
