package org.jgroups.raft.internal.serialization.binary;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.JGroupsRaftCustomMarshaller;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for binary serializers with optimized lookup for built-in types.
 *
 * <p>
 * This registry maintains bidirectional mappings between Java types and their serializers, enabling both serialization
 * (class → serializer) and deserialization (type-id → serializer) lookups. It implements a hybrid lookup strategy that
 * optimizes for the common case of built-in framework types while still supporting user-defined custom serializers.
 * </p>
 *
 * <h2>Lookup Strategy</h2>
 *
 * <p>
 * The registry uses two complementary lookup mechanisms:
 * </p>
 * <ul>
 *   <li><b>Array-based fast path:</b> Direct array indexing for built-in primitive and collection serializers.
 *       O(1) lookup with minimal overhead. This should include all internal types.</li>
 *   <li><b>HashMap fallback:</b> {@link ConcurrentHashMap} for user-defined serializers and less frequently used framework types.</li>
 * </ul>
 *
 * <h2>Type ID Allocation</h2>
 *
 * <p>
 * Type IDs are allocated in ranges for different purposes:
 * </p>
 * <ul>
 *   <li><b>0-31:</b> Fast-path primitives (optimized wire format, no version/length)</li>
 *   <li><b>32-999:</b> Framework types (collections, arrays, internal types)</li>
 *   <li><b>1000+:</b> User-defined types ({@link JGroupsRaftCustomMarshaller#MINIMUM_TYPE_ID})</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 * This class is thread-safe for concurrent registration and lookup:
 * </p>
 * <ul>
 *   <li>Registration uses {@link ConcurrentHashMap#putIfAbsent} for atomic insertion</li>
 *   <li>Array lookup is lock-free (array populated during registration)</li>
 *   <li>Map lookup is thread-safe via {@link ConcurrentHashMap}</li>
 * </ul>
 *
 * <p>
 * <b>Registration Pattern:</b> Serializers should be registered during initialization before any serialization operations
 * occur. Concurrent registration from multiple threads is safe but will result in exceptions if duplicate type IDs or classes
 * are detected.
 * </p>
 *
 * <h2>Error Handling</h2>
 *
 * <p>
 * The registry enforces strict uniqueness constraints:
 * </p>
 * <ul>
 *   <li>Duplicate class registration throws {@link IllegalStateException}</li>
 *   <li>Duplicate type ID registration throws {@link IllegalStateException}</li>
 *   <li>Lookup of unregistered type/ID throws {@link IllegalStateException}</li>
 * </ul>
 *
 * <p>
 * <b>Note:</b> This class is package-private and not intended for direct use by applications.
 * It is managed internally by {@link BinarySerializer}.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see SingleBinarySerializer
 * @see BinarySerializer
 * @see JGroupsRaftCustomMarshaller
 */
final class BinarySerializationRegistry {
    private final Map<Class<?>, SingleBinarySerializer<?>> classToSerializer;
    private final Map<Integer, SingleBinarySerializer<?>> idToSerializer;
    private final ArrayLookupRegistry fastRegistry;

    BinarySerializationRegistry() {
        this.classToSerializer = new ConcurrentHashMap<>();
        this.idToSerializer = new ConcurrentHashMap<>();
        this.fastRegistry = ArrayLookupRegistry.create(128);
    }

    public void registerSerializer(SingleBinarySerializer<?> serializer) {
        Objects.requireNonNull(serializer, "serializer may not be null");
        Objects.requireNonNull(serializer.javaClass(), "serializer class may not be null");

        SingleBinarySerializer<?> prev = classToSerializer.putIfAbsent(serializer.javaClass(), serializer);
        if (prev != null) {
            String message = "Serializer for class '%s' was already registered. "
                    + "The current implementation is '%s', the previous one is %s";
            throw new IllegalStateException(String.format(message, serializer.javaClass(), serializer.getClass(), prev.getClass()));
        }

        prev = idToSerializer.putIfAbsent(serializer.type(), serializer);
        if (prev != null) {
            String message = "Serializer with type id '%d' was already registered. "
                    + "The current implementation is '%s', the previous one is %s";
            throw new IllegalStateException(String.format(message, serializer.type(), serializer.javaClass(), prev.getClass()));
        }

        fastRegistry.registerSerializer(serializer);
    }

    public SingleBinarySerializer<?> findSerializer(Class<?> type) {
        SingleBinarySerializer<?> serializer = classToSerializer.get(type);
        if (serializer == null) {
            String message = "Serializer not found for type '%s'. "
                    + "You should implement the %s interface and register the custom serializer when "
                    + "building the JGroupsRaft instance.";
            throw new IllegalStateException(String.format(message, type, JGroupsRaftCustomMarshaller.class.getName()));
        }

        return serializer;
    }

    public SingleBinarySerializer<?> findDeserializer(int id) {
        SingleBinarySerializer<?> serializer = fastRegistry.findSerializer(id);
        if (serializer != null)
            return serializer;

        serializer = idToSerializer.get(id);
        if (serializer == null) {
            String message = "Serializer not found for id '%s'. "
                    + "You should implement the %s interface and register the custom serializer when "
                    + "building the JGroupsRaft instance.";
            throw new IllegalStateException(String.format(message, id, JGroupsRaftCustomMarshaller.class.getName()));
        }
        return serializer;
    }

    private record ArrayLookupRegistry(SingleBinarySerializer<?>[] lookup) {
        public static ArrayLookupRegistry create(int size) {
            return new ArrayLookupRegistry(new SingleBinarySerializer[size]);
        }

        public void registerSerializer(SingleBinarySerializer<?> serializer) {
            if (serializer.type() >= lookup.length)
                return;

            lookup[serializer.type()] = serializer;
        }

        public SingleBinarySerializer<?> findSerializer(int id) {
            if (id >= 0 && id < lookup.length)
                return lookup[id];

            return null;
        }
    }
}
