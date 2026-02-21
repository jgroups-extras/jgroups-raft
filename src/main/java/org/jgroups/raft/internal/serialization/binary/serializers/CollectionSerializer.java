package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.function.IntFunction;

/**
 * Internal serializers for Java collection types.
 *
 * <p>
 * This class provides serializers for common collection types:
 * </p>
 * <ul>
 *   <li>{@link ArrayList}</li>
 *   <li>{@link LinkedList}</li>
 *   <li>{@link HashSet}</li>
 *   <li>{@link LinkedHashSet}</li>
 *   <li>{@link TreeSet}</li>
 * </ul>
 *
 * <p>
 * Collections are serialized with a length prefix (int) followed by individual elements. Element serialization is delegated
 * to {@link SerializationContextWrite#writeObject(Object)}, which automatically handles primitives, nested objects, and null values.
 * </p>
 *
 * <p>
 * The serialization format is:
 * </p>
 * <pre>
 * [size: int][element1][element2]...[elementN]
 * </pre>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class CollectionSerializer {

    static final SingleBinarySerializer<?>[] SERIALIZERS = {
            ArrayListSerializer.INSTANCE,
            LinkedListSerializer.INSTANCE,
            HashSetSerializer.INSTANCE,
            LinkedHashSetSerializer.INSTANCE,
            TreeSetSerializer.INSTANCE
    };

    private CollectionSerializer() { }

    /**
     * Base class for collection serializers.
     *
     * <p>
     * This abstraction handles collection serialization by writing/reading the size followed by individual elements using
     * {@link SerializationContextWrite#writeObject(Object)} and {@link SerializationContextRead#readObject()}. This allows
     * automatic delegation to the appropriate serializer for each element type.
     * </p>
     *
     * @param <T> The collection type
     */
    private abstract static class AbstractCollectionSerializer<T extends Collection<?>> implements SingleBinarySerializer<T> {

        private final int type;
        private final Class<T> clazz;
        private final IntFunction<T> collectionFactory;

        /**
         * Creates a new collection serializer.
         *
         * @param type The unique type ID
         * @param clazz The collection class
         * @param collectionFactory Function to create a new collection with the given initial capacity
         */
        protected AbstractCollectionSerializer(int type, Class<T> clazz, IntFunction<T> collectionFactory) {
            this.type = type;
            this.clazz = clazz;
            this.collectionFactory = collectionFactory;
        }

        @Override
        public void write(SerializationContextWrite ctx, T target) {
            ctx.writeInt(target.size());
            for (Object element : target) {
                ctx.writeObject(element);
            }
        }

        @Override
        public T read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            T collection = collectionFactory.apply(size);
            for (int i = 0; i < size; i++) {
                addElement(collection, ctx.readObject());
            }
            return collection;
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
         * Adds an element to the collection.
         *
         * <p>
         * This method is provided to allow type-safe addition despite type erasure. The default implementation uses an
         * unchecked cast.
         * </p>
         *
         * @param collection The collection
         * @param element The element to add
         */
        @SuppressWarnings("unchecked")
        protected void addElement(T collection, Object element) {
            ((Collection<Object>) collection).add(element);
        }
    }

    /**
     * Serializer for {@link ArrayList}.
     *
     * <p>
     * Creates an {@link ArrayList} with the deserialized size as the initial capacity to avoid resizing during deserialization.
     * </p>
     */
    private static final class ArrayListSerializer extends AbstractCollectionSerializer<ArrayList<?>> {
        static final ArrayListSerializer INSTANCE = new ArrayListSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private ArrayListSerializer() {
            super(PrimitiveTypeIds.ARRAY_LIST_TYPE_ID,
                    (Class<ArrayList<?>>) (Class) ArrayList.class,
                    ArrayList::new);
        }
    }

    /**
     * Serializer for {@link LinkedList}.
     */
    private static final class LinkedListSerializer extends AbstractCollectionSerializer<LinkedList<?>> {
        static final LinkedListSerializer INSTANCE = new LinkedListSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private LinkedListSerializer() {
            super(PrimitiveTypeIds.LINKED_LIST_TYPE_ID,
                    (Class<LinkedList<?>>) (Class) LinkedList.class,
                    size -> new LinkedList<>());
        }
    }

    /**
     * Serializer for {@link HashSet}.
     *
     * <p>
     * Creates a {@link HashSet} with the deserialized size as the initial capacity to avoid rehashing during deserialization.
     * </p>
     */
    private static final class HashSetSerializer extends AbstractCollectionSerializer<HashSet<?>> {
        static final HashSetSerializer INSTANCE = new HashSetSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private HashSetSerializer() {
            super(PrimitiveTypeIds.HASH_SET_TYPE_ID,
                    (Class<HashSet<?>>) (Class) HashSet.class,
                    HashSet::new);
        }
    }

    /**
     * Serializer for {@link LinkedHashSet}.
     *
     * <p>
     * Creates a {@link LinkedHashSet} with the deserialized size as the initial capacity to avoid rehashing during
     * deserialization. Maintains insertion order.
     * </p>
     */
    private static final class LinkedHashSetSerializer extends AbstractCollectionSerializer<LinkedHashSet<?>> {
        static final LinkedHashSetSerializer INSTANCE = new LinkedHashSetSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private LinkedHashSetSerializer() {
            super(PrimitiveTypeIds.LINKED_HASH_SET_TYPE_ID,
                    (Class<LinkedHashSet<?>>) (Class) LinkedHashSet.class,
                    LinkedHashSet::new);
        }
    }

    /**
     * Serializer for {@link TreeSet}.
     *
     * <p>
     * Creates a {@link TreeSet} which maintains natural ordering of elements. Elements must be comparable or a
     * {@link ClassCastException} will be thrown.
     * </p>
     */
    private static final class TreeSetSerializer extends AbstractCollectionSerializer<TreeSet<?>> {
        static final TreeSetSerializer INSTANCE = new TreeSetSerializer();

        @SuppressWarnings({"unchecked", "rawtypes"})
        private TreeSetSerializer() {
            super(PrimitiveTypeIds.TREE_SET_TYPE_ID,
                    (Class<TreeSet<?>>) (Class) TreeSet.class,
                    size -> new TreeSet<>());
        }
    }
}
