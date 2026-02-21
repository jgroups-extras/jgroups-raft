package org.jgroups.raft.internal.serialization;

import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

public interface SingleBinarySerializer<T> {

    /**
     * Writes the object to the output context.
     *
     * <p>
     * The serializer should write all necessary data to reconstruct the object.
     * Do NOT write the type ID - that's handled by the registry.
     * </p>
     *
     * @param ctx The write context
     * @param target The object to serialize (never null)
     */
    void write(SerializationContextWrite ctx, T target);

    /**
     * Reads an object from the input context.
     *
     * <p>
     * The serializer should read the same data written by {@link #write}.
     * The type ID has already been read by the registry.
     * </p>
     *
     * @param ctx The read context
     * @return The deserialized object (never null)
     */
    T read(SerializationContextRead ctx, byte version);

    /**
     * Returns the Java class this serializer handles.
     *
     * @return The class
     */
    Class<T> javaClass();

    /**
     * Returns the unique type ID of the current class.
     *
     * @return the type ID.
     */
    int type();

    byte version();
}
