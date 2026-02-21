package org.jgroups.raft.internal.serialization.binary;

import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.command.JGroupsRaftWriteCommandOptions;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.RaftCommand;
import org.jgroups.raft.internal.command.RaftResponse;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.internal.serialization.binary.serializers.InternalSerializers;
import org.jgroups.raft.internal.statemachine.StateMachineStateHolder;
import org.jgroups.raft.util.ClassUtil;
import org.jgroups.raft.util.io.CustomByteBuffer;
import org.jgroups.raft.util.io.CustomByteBufferPool;
import org.jgroups.util.ByteArrayDataOutputStream;

import java.io.DataOutput;

/**
 * Binary serializer implementation using registered {@link org.jgroups.raft.internal.serialization.SingleBinarySerializer} instances.
 *
 * <p>
 * This serializer uses a compact binary wire format:
 * </p>
 *
 * <pre>
 * Wire format for objects:
 * [type-id: int][version: byte][length: int][data]
 *
 * Wire format for null:
 * [type-id: int = -1]
 * </pre>
 *
 * <p>
 * The length field enables forward compatibility - when an old serializer reads data from a newer version, it can skip
 * unknown trailing bytes without corrupting the stream.
 * </p>
 *
 * @since 2.1
 * @author José Bolina
 */
public final class BinarySerializer implements Serializer {
    private static final int DEFAULT_BUFFER_SIZE = 1 << 11;
    public static final ThreadLocal<CustomByteBufferPool> LOCAL_POOL =
            InheritableThreadLocal.withInitial(() -> new CustomByteBufferPool(8, 32 * 1024));
    private static final ThreadLocal<SerializationSizeCache> SIZE_CACHE =
            InheritableThreadLocal.withInitial(SerializationSizeCache::new);

    static final SingleBinarySerializer<?>[] INTERNAL_SERIALIZERS = {
            RaftCommand.SERIALIZER,
            RaftResponse.SERIALIZER,
            StateMachineStateHolder.SERIALIZER,
            JRaftCommand.UserCommand.SERIALIZER,
            JGroupsRaftReadCommandOptions.ReadImpl.SERIALIZER,
            JGroupsRaftWriteCommandOptions.WriteImpl.SERIALIZER,
    };
    private final BinarySerializationRegistry registry;

    public BinarySerializer(SerializationRegistry registry) {
        this.registry = ((SerializationRegistry.DefaultSerializationRegistry) registry).get();
        registerInternalSerializers(this.registry);
        registerBuiltInSerializers(this.registry);
    }

    static void registerInternalSerializers(BinarySerializationRegistry registry) {
        for (SingleBinarySerializer<?> serializer : INTERNAL_SERIALIZERS) {
            registry.registerSerializer(serializer);
        }
    }

    /**
     * Registers built-in serializers for common Java types.
     *
     * <p>
     * This will include serializers for:
     * <ul>
     *   <li>Primitives: String, byte[], Integer, Long, Boolean, etc.</li>
     *   <li>Common types: java.time.Instant, java.util.Date, etc.</li>
     * </ul>
     * </p>
     */
    static void registerBuiltInSerializers(BinarySerializationRegistry registry) {
        for (SingleBinarySerializer<?> serializer : InternalSerializers.serializers()) {
            registry.registerSerializer(serializer);
        }
    }

    @Override
    public byte[] serialize(Object object) {
        SerializationSizeCache cache = SIZE_CACHE.get();

        boolean update = false;
        int size = DEFAULT_BUFFER_SIZE;
        if (shouldEstimateSize(object)) {
            update = true;
            size = Math.max(cache.lookup(object.getClass()), DEFAULT_BUFFER_SIZE);
        }

        CustomByteBufferPool pool = LOCAL_POOL.get();
        CustomByteBuffer buffer = pool.acquire(size);
        try {
            // Create serialization context
            DefaultSerializationContext ctx = new DefaultSerializationContext(registry, buffer);

            // Write object (handles null, type ID, version, length, and data)
            ctx.writeObject(object);
            int actualSize = buffer.position();
            if (update && actualSize != size)
                cache.update(object.getClass(), actualSize);
            return buffer.toByteArray();
        } finally {
            pool.release(buffer);
        }
    }

    @Override
    public void serialize(DataOutput output, Object object) {
        if (!(output instanceof ByteArrayDataOutputStream baos)) {
            throw new IllegalArgumentException(
                    "DataOutput must be ByteArrayDataOutputStream, got: " + output.getClass());
        }

        SerializationSizeCache cache = SIZE_CACHE.get();
        boolean update = false;
        int size = DEFAULT_BUFFER_SIZE;
        if (shouldEstimateSize(object)) {
            update = true;
            size = Math.max(cache.lookup(object.getClass()), DEFAULT_BUFFER_SIZE);
        }

        if (baos.capacity() < DEFAULT_BUFFER_SIZE) {
            baos.growExponentially(false);
            baos.ensureCapacity(size);
        }

        ByteArrayDataOutputSerializationContextWrite ctx = new ByteArrayDataOutputSerializationContextWrite(registry, baos);
        ctx.writeObject(object);

        int actualSize = baos.position();
        if (update && actualSize != size)
            cache.update(object.getClass(), actualSize);
    }

    private boolean shouldEstimateSize(Object object) {
        if (object == null) return false;
        if (object instanceof Number) return false;
        if (object instanceof String str) return str.length() > 512;
        if (object instanceof byte[] arr) return arr.length + 8 > DEFAULT_BUFFER_SIZE;

        return true;
    }

    @Override
    public <T> T deserialize(byte[] buffer, Class<T> clazz) {
        if (buffer == null || buffer.length == 0) {
            return null;
        }

        // Wrap buffer for reading
        CustomByteBuffer byteBuffer = CustomByteBuffer.wrap(buffer);

        // Create deserialization context
        DefaultSerializationContext ctx = new DefaultSerializationContext(registry, byteBuffer);

        // Read object (handles null, type ID, version, length, and data)
        T result = ctx.readObject();

        // Verify type matches expectation
        if (result != null && !ClassUtil.isEquivalent(clazz, result.getClass())) {
            String msg = "Deserializing buffer expected type '%s' but found '%s', serialized is %s";
            throw new IllegalArgumentException(String.format(msg, clazz.getName(), result.getClass().getName(), result));
        }

        return result;
    }
}
