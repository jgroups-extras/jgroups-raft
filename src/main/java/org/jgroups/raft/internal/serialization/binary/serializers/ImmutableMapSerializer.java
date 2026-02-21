package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Internal serializers for immutable map types.
 *
 * <p>
 * This class provides serializers for JVM-provided immutable maps:
 * </p>
 * <ul>
 *   <li>{@link Collections#emptyMap()}</li>
 *   <li>{@link Collections#singletonMap(Object, Object)}</li>
 *   <li>{@link Map#of(Object, Object, ...)}</li>
 *   <li>{@link Collections#unmodifiableMap(Map)}</li>
 * </ul>
 *
 * <p>
 * These serializers preserve immutability by deserializing back to the appropriate factory methods.
 * This ensures that immutable maps remain immutable across serialization.
 * </p>
 *
 * <p>
 * The wire format for non-empty maps is:
 * </p>
 * <pre>
 * [size: int][key1][value1][key2][value2]...[keyN][valueN]
 * </pre>
 *
 * <p>
 * Empty maps have no additional data beyond the type header.
 * </p>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class ImmutableMapSerializer {

    // Known immutable map classes for type detection
    private static final Class<?> EMPTY_MAP_CLASS = Collections.emptyMap().getClass();
    private static final Class<?> SINGLETON_MAP_CLASS = Collections.singletonMap(null, null).getClass();
    private static final Class<?> UNMODIFIABLE_MAP_CLASS = Collections.unmodifiableMap(new HashMap<>()).getClass();

    // Map.of() returns different internal classes based on size
    private static final Class<?> MAP_OF_N_CLASS = Map.of().getClass();
    private static final Class<?> MAP_OF_1_CLASS = Map.of("", "").getClass();

    static final SingleBinarySerializer<?>[] SERIALIZERS = {
            EmptyMapSerializer.INSTANCE,
            SingletonMapSerializer.INSTANCE,
            UnmodifiableMapSerializer.INSTANCE,
            MapOf1Serializer.INSTANCE,
            MapOfNSerializer.INSTANCE
    };

    private ImmutableMapSerializer() { }

    /**
     * Serializer for {@link Collections#emptyMap()}.
     *
     * <p>
     * No entry data is written since the map is always empty.
     * </p>
     */
    private static final class EmptyMapSerializer implements SingleBinarySerializer<Map<?, ?>> {
        static final EmptyMapSerializer INSTANCE = new EmptyMapSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Map<?, ?> target) {
            // No data to write for empty map
        }

        @Override
        public Map<?, ?> read(SerializationContextRead ctx, byte version) {
            return Collections.emptyMap();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Map<?, ?>> javaClass() {
            return (Class<Map<?, ?>>) EMPTY_MAP_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.EMPTY_MAP_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Collections#singletonMap(Object, Object)}.
     *
     * <p>
     * Writes only the single key-value pair, avoiding the size prefix since it's always 1.
     * </p>
     */
    private static final class SingletonMapSerializer implements SingleBinarySerializer<Map<?, ?>> {
        static final SingletonMapSerializer INSTANCE = new SingletonMapSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Map<?, ?> target) {
            Map.Entry<?, ?> entry = target.entrySet().iterator().next();
            ctx.writeObject(entry.getKey());
            ctx.writeObject(entry.getValue());
        }

        @Override
        public Map<?, ?> read(SerializationContextRead ctx, byte version) {
            Object key = ctx.readObject();
            Object value = ctx.readObject();
            return Collections.singletonMap(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Map<?, ?>> javaClass() {
            return (Class<Map<?, ?>>) SINGLETON_MAP_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.SINGLETON_MAP_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Collections#unmodifiableMap(Map)}.
     *
     * <p>
     * Serializes the map contents and wraps the deserialized result in an unmodifiable view.
     * </p>
     */
    private static final class UnmodifiableMapSerializer implements SingleBinarySerializer<Map<?, ?>> {
        static final UnmodifiableMapSerializer INSTANCE = new UnmodifiableMapSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Map<?, ?> target) {
            ctx.writeInt(target.size());
            for (Map.Entry<?, ?> entry : target.entrySet()) {
                ctx.writeObject(entry.getKey());
                ctx.writeObject(entry.getValue());
            }
        }

        @Override
        public Map<?, ?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            Map<Object, Object> map = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                Object key = ctx.readObject();
                Object value = ctx.readObject();
                map.put(key, value);
            }
            return Collections.unmodifiableMap(map);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Map<?, ?>> javaClass() {
            return (Class<Map<?, ?>>) UNMODIFIABLE_MAP_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.UNMODIFIABLE_MAP_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Map#of(Object, Object)} with 1 entry.
     *
     * <p>
     * Writes only the single key-value pair, avoiding the size prefix since it's always 1.
     * </p>
     */
    private static final class MapOf1Serializer implements SingleBinarySerializer<Map<?, ?>> {
        static final MapOf1Serializer INSTANCE = new MapOf1Serializer();

        @Override
        public void write(SerializationContextWrite ctx, Map<?, ?> target) {
            Map.Entry<?, ?> entry = target.entrySet().iterator().next();
            ctx.writeObject(entry.getKey());
            ctx.writeObject(entry.getValue());
        }

        @Override
        public Map<?, ?> read(SerializationContextRead ctx, byte version) {
            Object key = ctx.readObject();
            Object value = ctx.readObject();
            return Map.of(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Map<?, ?>> javaClass() {
            return (Class<Map<?, ?>>) MAP_OF_1_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.MAP_OF_1_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Map#of(Object, Object)} with 2+ entries.
     *
     * <p>
     * Writes the size prefix followed by all key-value pairs.
     * </p>
     */
    private static final class MapOfNSerializer implements SingleBinarySerializer<Map<?, ?>> {
        static final MapOfNSerializer INSTANCE = new MapOfNSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Map<?, ?> target) {
            ctx.writeInt(target.size());
            for (Map.Entry<?, ?> entry : target.entrySet()) {
                ctx.writeObject(entry.getKey());
                ctx.writeObject(entry.getValue());
            }
        }

        @Override
        public Map<?, ?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            Object[] keysAndValues = new Object[size * 2];
            for (int i = 0; i < size; i++) {
                keysAndValues[i * 2] = ctx.readObject();
                keysAndValues[i * 2 + 1] = ctx.readObject();
            }

            // Map.of() takes alternating key-value pairs
            return switch (size) {
                case 2 -> Map.of(keysAndValues[0], keysAndValues[1], keysAndValues[2], keysAndValues[3]);
                case 3 -> Map.of(keysAndValues[0], keysAndValues[1], keysAndValues[2], keysAndValues[3],
                        keysAndValues[4], keysAndValues[5]);
                case 4 -> Map.of(keysAndValues[0], keysAndValues[1], keysAndValues[2], keysAndValues[3],
                        keysAndValues[4], keysAndValues[5], keysAndValues[6], keysAndValues[7]);
                case 5 -> Map.of(keysAndValues[0], keysAndValues[1], keysAndValues[2], keysAndValues[3],
                        keysAndValues[4], keysAndValues[5], keysAndValues[6], keysAndValues[7],
                        keysAndValues[8], keysAndValues[9]);
                default -> {
                    // For 6+ entries, use Map.ofEntries
                    Map.Entry<?, ?>[] entries = new Map.Entry[size];
                    for (int i = 0; i < size; i++) {
                        entries[i] = Map.entry(keysAndValues[i * 2], keysAndValues[i * 2 + 1]);
                    }
                    yield Map.ofEntries(entries);
                }
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Map<?, ?>> javaClass() {
            return (Class<Map<?, ?>>) MAP_OF_N_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.MAP_OF_N_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
