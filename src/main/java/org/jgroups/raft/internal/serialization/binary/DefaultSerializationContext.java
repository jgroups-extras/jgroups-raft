package org.jgroups.raft.internal.serialization.binary;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.internal.serialization.binary.serializers.PrimitiveTypeIds;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.util.io.CustomByteBuffer;

import java.nio.charset.StandardCharsets;

public final class DefaultSerializationContext extends AbstractSerializationContextWrite implements SerializationContextRead {
    private static final int NULL_OBJECT = -1;

    private final CustomByteBuffer buffer;

    DefaultSerializationContext(BinarySerializationRegistry registry, CustomByteBuffer buffer) {
        super(registry, buffer);
        this.buffer = buffer;
    }

    @Override
    public <T> T readObject() {
        int typeId = buffer.readInt();

        if (typeId == NULL_OBJECT) {
            return null;
        }

        // Fast path: primitive types use optimized wire format without version/length
        if (typeId <= PrimitiveTypeIds.FAST_PATH_MAX_TYPE_ID) {
            return readPrimitive(typeId);
        }

        // Slow path: complex objects with version and length
        @SuppressWarnings("unchecked")
        SingleBinarySerializer<T> serializer = (SingleBinarySerializer<T>) registry.findDeserializer(typeId);
        byte version = buffer.readByte();
        int length = buffer.readInt();
        int startPos = buffer.position();
        T result = serializer.read(this, version);
        int bytesRead = buffer.position() - startPos;
        int remainingBytes = length - bytesRead;

        if (remainingBytes == 0)
            return result;

        if (remainingBytes > 0) {
            buffer.skip(remainingBytes);
            return result;
        }

        String message = "Serializer for '%s' has read more bytes than expected. "
                + "It should read %d but read an extra %d bytes";
        throw new IllegalStateException(String.format(message, serializer.javaClass(), length, -remainingBytes));
    }

    /**
     * Fast-path deserialization for primitive types.
     * These types use optimized wire format: [type-id][data]
     * No version or length fields.
     */
    @SuppressWarnings("unchecked")
    private <T> T readPrimitive(int typeId) {
        Object o = switch (typeId) {
            case PrimitiveTypeIds.BYTE_TYPE_ID -> buffer.readByte();
            case PrimitiveTypeIds.SHORT_TYPE_ID -> buffer.readShort();
            case PrimitiveTypeIds.INTEGER_TYPE_ID -> buffer.readInt();
            case PrimitiveTypeIds.LONG_TYPE_ID -> buffer.readLong();
            case PrimitiveTypeIds.FLOAT_TYPE_ID -> buffer.readFloat();
            case PrimitiveTypeIds.DOUBLE_TYPE_ID -> buffer.readDouble();
            case PrimitiveTypeIds.BOOLEAN_TYPE_ID -> buffer.readBoolean();
            case PrimitiveTypeIds.CHARACTER_TYPE_ID -> (char) buffer.readShort();
            case PrimitiveTypeIds.STRING_TYPE_ID -> new String(buffer.readBytes(buffer.readShort()), StandardCharsets.UTF_8);
            case PrimitiveTypeIds.BYTE_ARRAY_TYPE_ID -> buffer.readBytes(buffer.readInt());
            default -> throw new IllegalArgumentException("Unknown primitive type ID: " + typeId);
        };
        return (T) o;
    }

    @Override
    public byte readByte() {
        return buffer.readByte();
    }

    @Override
    public short readShort() {
        return buffer.readShort();
    }

    @Override
    public int readInt() {
        return buffer.readInt();
    }

    @Override
    public long readLong() {
        return buffer.readLong();
    }

    @Override
    public boolean readBoolean() {
        return buffer.readBoolean();
    }

    @Override
    public float readFloat() {
        return buffer.readFloat();
    }

    @Override
    public double readDouble() {
        return buffer.readDouble();
    }

    @Override
    public String readUTF() {
        return buffer.readUTF();
    }

    @Override
    public byte[] readBytes(int length) {
        return buffer.readBytes(length);
    }

    @Override
    public void readBytes(byte[] b) {
        buffer.readBytes(b);
    }

    @Override
    public void readBytes(byte[] b, int off, int len) {
        buffer.readBytes(b, off, len);
    }
}
