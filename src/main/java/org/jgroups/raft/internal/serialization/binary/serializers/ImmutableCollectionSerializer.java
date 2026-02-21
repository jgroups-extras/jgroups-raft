package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Internal serializers for immutable collection types.
 *
 * <p>
 * This class provides serializers for JVM-provided immutable collections:
 * </p>
 * <ul>
 *   <li>{@link Collections#emptyList()}, {@link Collections#emptySet()}</li>
 *   <li>{@link Collections#singletonList(Object)}, {@link Collections#singleton(Object)}</li>
 *   <li>{@link List#of(Object...)}, {@link Set#of(Object...)}</li>
 *   <li>{@link Collections#unmodifiableList(List)}, {@link Collections#unmodifiableSet(Set)}</li>
 * </ul>
 *
 * <p>
 * These serializers preserve immutability by deserializing back to the appropriate factory methods. This ensures that
 * immutable collections remain immutable across serialization.
 * </p>
 *
 * <p>
 * The wire format for non-empty collections is:
 * </p>
 * <pre>
 * [size: int][element1][element2]...[elementN]
 * </pre>
 *
 * <p>
 * Empty collections have no additional data beyond the type header.
 * </p>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class ImmutableCollectionSerializer {

    // Known immutable collection classes for type detection
    private static final Class<?> EMPTY_LIST_CLASS = Collections.emptyList().getClass();
    private static final Class<?> EMPTY_SET_CLASS = Collections.emptySet().getClass();
    private static final Class<?> SINGLETON_LIST_CLASS = Collections.singletonList(null).getClass();
    private static final Class<?> SINGLETON_SET_CLASS = Collections.singleton(null).getClass();
    private static final Class<?> UNMODIFIABLE_LIST_CLASS = Collections.unmodifiableList(new ArrayList<>()).getClass();
    private static final Class<?> UNMODIFIABLE_SET_CLASS = Collections.unmodifiableSet(new HashSet<>()).getClass();

    // List.of() and Set.of() return different internal classes based on size,
    // so we detect multiple sizes to handle all variants
    private static final Class<?> LIST_OF_N_CLASS = List.of().getClass();
    private static final Class<?> LIST_OF_1_CLASS = List.of("").getClass();

    private static final Class<?> SET_OF_N_CLASS = Set.of().getClass();
    private static final Class<?> SET_OF_1_CLASS = Set.of("").getClass();

    static final SingleBinarySerializer<?>[] SERIALIZERS = {
            EmptyListSerializer.INSTANCE,
            EmptySetSerializer.INSTANCE,
            SingletonListSerializer.INSTANCE,
            SingletonSetSerializer.INSTANCE,
            UnmodifiableListSerializer.INSTANCE,
            UnmodifiableSetSerializer.INSTANCE,
            ListOf12Serializer.INSTANCE,
            ListOfNSerializer.INSTANCE,
            SetOf12Serializer.INSTANCE,
            SetOfNSerializer.INSTANCE
    };

    private ImmutableCollectionSerializer() { }

    /**
     * Serializer for {@link Collections#emptyList()}.
     *
     * <p>
     * No element data is written since the list is always empty.
     * </p>
     */
    private static final class EmptyListSerializer implements SingleBinarySerializer<List<?>> {
        static final EmptyListSerializer INSTANCE = new EmptyListSerializer();

        @Override
        public void write(SerializationContextWrite ctx, List<?> target) {
            // No data to write for empty list
        }

        @Override
        public List<?> read(SerializationContextRead ctx, byte version) {
            return Collections.emptyList();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<List<?>> javaClass() {
            return (Class<List<?>>) EMPTY_LIST_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.EMPTY_LIST_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Collections#emptySet()}.
     *
     * <p>
     * No element data is written since the set is always empty.
     * </p>
     */
    private static final class EmptySetSerializer implements SingleBinarySerializer<Set<?>> {
        static final EmptySetSerializer INSTANCE = new EmptySetSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Set<?> target) {
            // No data to write for empty set
        }

        @Override
        public Set<?> read(SerializationContextRead ctx, byte version) {
            return Collections.emptySet();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Set<?>> javaClass() {
            return (Class<Set<?>>) EMPTY_SET_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.EMPTY_SET_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Collections#singletonList(Object)}.
     *
     * <p>
     * Writes only the single element, avoiding the size prefix since it's always 1.
     * </p>
     */
    private static final class SingletonListSerializer implements SingleBinarySerializer<List<?>> {
        static final SingletonListSerializer INSTANCE = new SingletonListSerializer();

        @Override
        public void write(SerializationContextWrite ctx, List<?> target) {
            ctx.writeObject(target.get(0));
        }

        @Override
        public List<?> read(SerializationContextRead ctx, byte version) {
            Object element = ctx.readObject();
            return Collections.singletonList(element);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<List<?>> javaClass() {
            return (Class<List<?>>) SINGLETON_LIST_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.SINGLETON_LIST_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Collections#singleton(Object)}.
     *
     * <p>
     * Writes only the single element, avoiding the size prefix since it's always 1.
     * </p>
     */
    private static final class SingletonSetSerializer implements SingleBinarySerializer<Set<?>> {
        static final SingletonSetSerializer INSTANCE = new SingletonSetSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Set<?> target) {
            ctx.writeObject(target.iterator().next());
        }

        @Override
        public Set<?> read(SerializationContextRead ctx, byte version) {
            Object element = ctx.readObject();
            return Collections.singleton(element);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Set<?>> javaClass() {
            return (Class<Set<?>>) SINGLETON_SET_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.SINGLETON_SET_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Collections#unmodifiableList(List)}.
     *
     * <p>
     * Serializes the list contents and wraps the deserialized result in an unmodifiable view.
     * </p>
     */
    private static final class UnmodifiableListSerializer implements SingleBinarySerializer<List<?>> {
        static final UnmodifiableListSerializer INSTANCE = new UnmodifiableListSerializer();

        @Override
        public void write(SerializationContextWrite ctx, List<?> target) {
            ctx.writeInt(target.size());
            for (Object element : target) {
                ctx.writeObject(element);
            }
        }

        @Override
        public List<?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            List<Object> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(ctx.readObject());
            }
            return Collections.unmodifiableList(list);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<List<?>> javaClass() {
            return (Class<List<?>>) UNMODIFIABLE_LIST_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.UNMODIFIABLE_LIST_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Collections#unmodifiableSet(Set)}.
     *
     * <p>
     * Serializes the set contents and wraps the deserialized result in an unmodifiable view.
     * </p>
     */
    private static final class UnmodifiableSetSerializer implements SingleBinarySerializer<Set<?>> {
        static final UnmodifiableSetSerializer INSTANCE = new UnmodifiableSetSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Set<?> target) {
            ctx.writeInt(target.size());
            for (Object element : target) {
                ctx.writeObject(element);
            }
        }

        @Override
        public Set<?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            Set<Object> set = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                set.add(ctx.readObject());
            }
            return Collections.unmodifiableSet(set);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Set<?>> javaClass() {
            return (Class<Set<?>>) UNMODIFIABLE_SET_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.UNMODIFIABLE_SET_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link List#of(Object)} with 1 element.
     *
     * <p>
     * Writes only the single element, avoiding the size prefix since it's always 1.
     * </p>
     */
    private static final class ListOf12Serializer implements SingleBinarySerializer<List<?>> {
        static final ListOf12Serializer INSTANCE = new ListOf12Serializer();

        @Override
        public void write(SerializationContextWrite ctx, List<?> target) {
            int size = target.size();
            ctx.writeInt(size);
            ctx.writeObject(target.get(0));

            if (size > 1)
                ctx.writeObject(target.get(1));
        }

        @Override
        public List<?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            Object element = ctx.readObject();
            if (size == 1)
                return List.of(element);

            return List.of(element, ctx.readObject());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<List<?>> javaClass() {
            return (Class<List<?>>) LIST_OF_1_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.LIST_OF_1_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link List#of(Object...)} with 3+ elements.
     *
     * <p>
     * Writes the size prefix followed by all elements.
     * </p>
     */
    private static final class ListOfNSerializer implements SingleBinarySerializer<List<?>> {
        static final ListOfNSerializer INSTANCE = new ListOfNSerializer();

        @Override
        public void write(SerializationContextWrite ctx, List<?> target) {
            ctx.writeInt(target.size());
            for (Object element : target) {
                ctx.writeObject(element);
            }
        }

        @Override
        public List<?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            if (size == 0)
                return List.of();

            Object[] elements = new Object[size];
            for (int i = 0; i < size; i++) {
                elements[i] = ctx.readObject();
            }
            return List.of(elements);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<List<?>> javaClass() {
            return (Class<List<?>>) LIST_OF_N_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.LIST_OF_N_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Set#of(Object)} with 1 element.
     *
     * <p>
     * Writes only the single element, avoiding the size prefix since it's always 1.
     * </p>
     */
    private static final class SetOf12Serializer implements SingleBinarySerializer<Set<?>> {
        static final SetOf12Serializer INSTANCE = new SetOf12Serializer();

        @Override
        public void write(SerializationContextWrite ctx, Set<?> target) {
            ctx.writeInt(target.size());

            Iterator<?> it = target.iterator();
            ctx.writeObject(it.next());
            if (it.hasNext())
                ctx.writeObject(it.next());
        }

        @Override
        public Set<?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            Object element = ctx.readObject();
            if (size == 1)
                return Set.of(element);

            return Set.of(element, ctx.readObject());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Set<?>> javaClass() {
            return (Class<Set<?>>) SET_OF_1_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.SET_OF_1_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Serializer for {@link Set#of(Object...)} with 3+ elements.
     *
     * <p>
     * Writes the size prefix followed by all elements.
     * </p>
     */
    private static final class SetOfNSerializer implements SingleBinarySerializer<Set<?>> {
        static final SetOfNSerializer INSTANCE = new SetOfNSerializer();

        @Override
        public void write(SerializationContextWrite ctx, Set<?> target) {
            ctx.writeInt(target.size());
            for (Object element : target) {
                ctx.writeObject(element);
            }
        }

        @Override
        public Set<?> read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            if (size == 0)
                return Set.of();

            Object[] elements = new Object[size];
            for (int i = 0; i < size; i++) {
                elements[i] = ctx.readObject();
            }
            return Set.of(elements);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Set<?>> javaClass() {
            return (Class<Set<?>>) SET_OF_N_CLASS;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.SET_OF_N_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
