package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;

/**
 * Internal serializers for Java map types.
 *
 * <p>
 * This class provides serializers for common map types:
 * </p>
 * <ul>
 *   <li>{@link HashMap}</li>
 *   <li>{@link LinkedHashMap}</li>
 *   <li>{@link TreeMap}</li>
 * </ul>
 *
 * <p>
 * Maps are serialized with a size prefix (int) followed by key-value pairs. Both keys and values are serialized using
 * {@link SerializationContextWrite#writeObject(Object)}, which automatically handles primitives, nested objects, and null values.
 * </p>
 *
 * <p>
 * The serialization format is:
 * </p>
 * <pre>
 * [size: int][key1][value1][key2][value2]...[keyN][valueN]
 * </pre>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class MapSerializer {

    static final SingleBinarySerializer<?>[] SERIALIZERS = {
            HashMapSerializer.INSTANCE,
            LinkedHashMapSerializer.INSTANCE,
            TreeMapSerializer.INSTANCE
    };

    private MapSerializer() { }

    /**
     * Base class for map serializers.
     *
     * <p>
     * This abstraction handles map serialization by writing/reading the size followed by key-value pairs using
     * {@link SerializationContextWrite#writeObject(Object)} and {@link SerializationContextRead#readObject()}. This allows
     * automatic delegation to the appropriate serializer for each key and value type.
     * </p>
     *
     * @param <T> The map type
     */
    private abstract static class AbstractMapSerializer<T extends Map<?, ?>> implements SingleBinarySerializer<T> {

        private final int type;
        private final Class<T> clazz;
        private final IntFunction<T> mapFactory;

        /**
         * Creates a new map serializer.
         *
         * @param type The unique type ID
         * @param clazz The map class
         * @param mapFactory Function to create a new map with the given initial capacity
         */
        protected AbstractMapSerializer(int type, Class<T> clazz, IntFunction<T> mapFactory) {
            this.type = type;
            this.clazz = clazz;
            this.mapFactory = mapFactory;
        }

        @Override
        public void write(SerializationContextWrite ctx, T target) {
            ctx.writeInt(target.size());
            for (Map.Entry<?, ?> entry : target.entrySet()) {
                ctx.writeObject(entry.getKey());
                ctx.writeObject(entry.getValue());
            }
        }

        @Override
        public T read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            T map = mapFactory.apply(size);
            for (int i = 0; i < size; i++) {
                Object key = ctx.readObject();
                Object value = ctx.readObject();
                putEntry(map, key, value);
            }
            return map;
        }

        @Override
        public Class<T> javaClass() {
            return clazz;
        }

        @Override
        public int type() {
            return type;
        }

        @Override
        public byte version() {
            return 0;
        }

        /**
         * Puts a key-value pair into the map.
         *
         * <p>
         * This method is provided to allow type-safe insertion despite type erasure. The default implementation uses an
         * unchecked cast.
         * </p>
         *
         * @param map The map
         * @param key The key
         * @param value The value
         */
        @SuppressWarnings("unchecked")
        protected void putEntry(T map, Object key, Object value) {
            ((Map<Object, Object>) map).put(key, value);
        }
    }

    /**
     * Serializer for {@link HashMap}.
     *
     * <p>
     * Creates a {@link HashMap} with the deserialized size as the initial capacity to avoid rehashing during deserialization.
     * </p>
     */
    private static final class HashMapSerializer extends AbstractMapSerializer<HashMap<?, ?>> {
        static final HashMapSerializer INSTANCE = new HashMapSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private HashMapSerializer() {
            super(PrimitiveTypeIds.HASH_MAP_TYPE_ID,
                    (Class<HashMap<?, ?>>) (Class) HashMap.class,
                    HashMap::new);
        }
    }

    /**
     * Serializer for {@link LinkedHashMap}.
     *
     * <p>
     * Creates a {@link LinkedHashMap} with the deserialized size as the initial capacity to avoid rehashing during
     * deserialization. Maintains insertion order.
     * </p>
     */
    private static final class LinkedHashMapSerializer extends AbstractMapSerializer<LinkedHashMap<?, ?>> {
        static final LinkedHashMapSerializer INSTANCE = new LinkedHashMapSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private LinkedHashMapSerializer() {
            super(PrimitiveTypeIds.LINKED_HASH_MAP_TYPE_ID,
                    (Class<LinkedHashMap<?, ?>>) (Class) LinkedHashMap.class,
                    LinkedHashMap::new);
        }
    }

    /**
     * Serializer for {@link TreeMap}.
     *
     * <p>
     * Creates a {@link TreeMap} which maintains keys in sorted order. Keys must be comparable or a {@link ClassCastException}
     * will be thrown.
     * </p>
     */
    private static final class TreeMapSerializer extends AbstractMapSerializer<TreeMap<?, ?>> {
        static final TreeMapSerializer INSTANCE = new TreeMapSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private TreeMapSerializer() {
            super(PrimitiveTypeIds.TREE_MAP_TYPE_ID,
                    (Class<TreeMap<?, ?>>) (Class) TreeMap.class,
                    size -> new TreeMap<>());
        }
    }
}
