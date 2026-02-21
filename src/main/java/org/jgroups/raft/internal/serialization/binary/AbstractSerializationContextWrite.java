package org.jgroups.raft.internal.serialization.binary;

import org.jgroups.raft.exceptions.JGroupsRaftSerializationException;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.internal.serialization.binary.serializers.PrimitiveTypeIds;
import org.jgroups.raft.serialization.SerializationContextWrite;
import org.jgroups.raft.util.io.RandomAccessOutput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Abstract base class implementing the binary wire format for object serialization.
 *
 * <p>
 * This class defines the complete serialization wire format logic once, ensuring consistency across different output backends.
 * It delegates actual I/O operations to a {@link RandomAccessOutput} implementation, which can be backed by
 * {@link org.jgroups.raft.util.io.CustomByteBuffer} for normal operations or {@link org.jgroups.util.ByteArrayDataOutputStream}
 * for zero-copy streaming.
 * </p>
 *
 * <h2>Wire Format</h2>
 *
 * <p>
 * The wire format uses two different encodings depending on the object type:
 * </p>
 *
 * <h3>Fast-Path Primitives (Type IDs 0-31):</h3>
 * <pre>
 * [type-id: 4 bytes][data: variable]
 * </pre>
 * <p>
 * No version or length fields for optimal performance. Supported types:
 * {@code Byte, Short, Integer, Long, Float, Double, Boolean, Character, String, byte[]}.
 * </p>
 *
 * <h3>Complex Objects (Type IDs 32+):</h3>
 * <pre>
 * [type-id: 4 bytes][version: 1 byte][length: 4 bytes][data: variable]
 * </pre>
 * <p>
 * The length field enables forward compatibility by allowing readers to skip unknown fields. This requires
 * {@link RandomAccessOutput} to patch the length after writing.
 * </p>
 *
 * <h3>Null Objects:</h3>
 * <pre>
 * [type-id: -1 (4 bytes)]
 * </pre>
 *
 * <h2>Subclass Responsibilities</h2>
 *
 * <p>
 * Subclasses only need to provide the appropriate {@code RandomAccessOutput} implementation in their constructor. All
 * wire format logic is handled by this base class.
 * </p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 * Instances are not thread-safe and should not be shared across threads. Each serialization operation should use a
 * dedicated instance.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see DefaultSerializationContext
 * @see ByteArrayDataOutputSerializationContextWrite
 * @see RandomAccessOutput
 */
public abstract sealed class AbstractSerializationContextWrite implements SerializationContextWrite
        permits DefaultSerializationContext, ByteArrayDataOutputSerializationContextWrite {

    private static final int NULL_OBJECT = -1;

    final BinarySerializationRegistry registry;
    private final RandomAccessOutput out;

    AbstractSerializationContextWrite(BinarySerializationRegistry registry, RandomAccessOutput out) {
        this.registry = registry;
        this.out = out;
    }

    @Override
    public final void writeObject(Object obj) {
        try {
            if (obj == null) {
                out.writeInt(NULL_OBJECT);
                return;
            }

            // Fast path: primitive types use optimized wire format without version/length
            if (tryFastPathSerialization(obj))
                return;

            // Slow path: complex objects with version and length
            @SuppressWarnings("unchecked")
            SingleBinarySerializer<Object> serializer =
                    (SingleBinarySerializer<Object>) registry.findSerializer(obj.getClass());

            out.writeInt(serializer.type());
            out.writeByte(serializer.version());

            // Reserve space for length (we'll patch it later)
            int lengthPosition = out.position();
            out.writeInt(0);  // Placeholder

            // Record position AFTER the length field
            int dataStartPosition = out.position();

            // Write the actual data
            serializer.write(this, obj);

            // Calculate actual data length
            int currentPosition = out.position();
            int dataLength = currentPosition - dataStartPosition;

            // Patch the length field in-place
            // Save current position, write length, restore position
            int savedPosition = currentPosition;
            out.position(lengthPosition);
            out.writeInt(dataLength);
            out.position(savedPosition);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    /**
     * Fast-path serialization for primitive types.
     * Identical to DefaultSerializationContext but writes directly to ByteArrayDataOutputStream.
     */
    private boolean tryFastPathSerialization(Object obj) throws IOException {
        if (obj instanceof byte[] arr) {
            out.writeInt(PrimitiveTypeIds.BYTE_ARRAY_TYPE_ID);
            out.writeInt(arr.length);
            out.write(arr);
            return true;
        }
        if (obj instanceof Integer i) {
            out.writeInt(PrimitiveTypeIds.INTEGER_TYPE_ID);
            out.writeInt(i);
            return true;
        }
        if (obj instanceof Byte b) {
            out.writeInt(PrimitiveTypeIds.BYTE_TYPE_ID);
            out.writeByte(b);
            return true;
        }
        if (obj instanceof Short s) {
            out.writeInt(PrimitiveTypeIds.SHORT_TYPE_ID);
            out.writeShort(s);
            return true;
        }
        if (obj instanceof Long l) {
            out.writeInt(PrimitiveTypeIds.LONG_TYPE_ID);
            out.writeLong(l);
            return true;
        }
        if (obj instanceof Float f) {
            out.writeInt(PrimitiveTypeIds.FLOAT_TYPE_ID);
            out.writeFloat(f);
            return true;
        }
        if (obj instanceof Double d) {
            out.writeInt(PrimitiveTypeIds.DOUBLE_TYPE_ID);
            out.writeDouble(d);
            return true;
        }
        if (obj instanceof Boolean b) {
            out.writeInt(PrimitiveTypeIds.BOOLEAN_TYPE_ID);
            out.writeBoolean(b);
            return true;
        }
        if (obj instanceof Character c) {
            out.writeInt(PrimitiveTypeIds.CHARACTER_TYPE_ID);
            out.writeShort(c);
            return true;
        }
        if (obj instanceof String str) {
            byte[] utf8 = str.getBytes(StandardCharsets.UTF_8);
            if (utf8.length <= Short.MAX_VALUE) {
                out.writeInt(PrimitiveTypeIds.STRING_TYPE_ID);
                out.writeShort(utf8.length);
                out.write(utf8);
                return true;
            }
        }

        return false;
    }

    // All primitive write methods delegate directly to ByteArrayDataOutputStream

    @Override
    public final void writeByte(int v) {
        try {
            out.writeByte(v);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing byte", e);
        }
    }

    @Override
    public final void writeShort(int v) {
        try {
            out.writeShort(v);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing short", e);
        }
    }

    @Override
    public final void writeInt(int v) {
        try {
            out.writeInt(v);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing int", e);
        }
    }

    @Override
    public final void writeLong(long v) {
        try {
            out.writeLong(v);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing long", e);
        }
    }

    @Override
    public final void writeBoolean(boolean v) {
        try {
            out.writeBoolean(v);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing boolean", e);
        }
    }

    @Override
    public final void writeFloat(float v) {
        try {
            out.writeFloat(v);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing float", e);
        }
    }

    @Override
    public final void writeDouble(double v) {
        try {
            out.writeDouble(v);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing double", e);
        }
    }

    @Override
    public final void writeUTF(String s) {
        try {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            if (utf8.length > Short.MAX_VALUE) {
                throw new IllegalArgumentException("String too long for UTF encoding: " + utf8.length);
            }
            out.writeShort(utf8.length);
            out.write(utf8);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing UTF string", e);
        }
    }

    @Override
    public final void writeBytes(byte[] b) {
        try {
            out.write(b);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing bytes", e);
        }
    }

    @Override
    public final void writeBytes(byte[] b, int off, int len) {
        try {
            out.write(b, off, len);
        } catch (IOException e) {
            throw new JGroupsRaftSerializationException("Failed writing bytes", e);
        }
    }
}
