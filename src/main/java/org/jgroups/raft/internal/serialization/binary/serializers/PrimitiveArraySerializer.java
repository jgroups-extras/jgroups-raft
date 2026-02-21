package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.lang.reflect.Array;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Internal serializers for Java primitive array types.
 *
 * <p>
 * This class provides serializers for the following primitive array types:
 * </p>
 * <ul>
 *   <li>{@code byte[]}</li>
 *   <li>{@code short[]}</li>
 *   <li>{@code int[]}</li>
 *   <li>{@code long[]}</li>
 *   <li>{@code float[]}</li>
 *   <li>{@code double[]}</li>
 *   <li>{@code boolean[]}</li>
 *   <li>{@code char[]}</li>
 * </ul>
 *
 * <p>
 * Arrays are serialized with a length prefix (int) followed by the array elements. The {@code byte[]}
 * type uses the optimized bulk write/read methods from the serialization context, while other types
 * iterate and write/read individual elements.
 * </p>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class PrimitiveArraySerializer {

    static final SingleBinarySerializer<?>[] SERIALIZERS = {
            ByteArraySerializer.INSTANCE,
            ShortArraySerializer.INSTANCE,
            IntArraySerializer.INSTANCE,
            LongArraySerializer.INSTANCE,
            FloatArraySerializer.INSTANCE,
            DoubleArraySerializer.INSTANCE,
            BooleanArraySerializer.INSTANCE,
            CharArraySerializer.INSTANCE
    };

    private PrimitiveArraySerializer() { }

    /**
     * Base class for primitive array serializers that iterate over elements.
     *
     * <p>
     * This abstraction handles array serialization by writing/reading the length followed by
     * individual elements using provided write/read functions.
     * </p>
     *
     * @param <T> The primitive array type
     * @param <E> The boxed element type
     */
    private abstract static class AbstractPrimitiveArraySerializer<T, E> implements SingleBinarySerializer<T> {

        private final int type;
        private final Class<T> clazz;
        private final BiConsumer<SerializationContextWrite, E> elementWriter;
        private final Function<SerializationContextRead, E> elementReader;

        /**
         * Creates a new primitive array serializer.
         *
         * @param type The unique type ID
         * @param clazz The array class
         * @param elementWriter Function to write a single element
         * @param elementReader Function to read a single element
         */
        protected AbstractPrimitiveArraySerializer(int type, Class<T> clazz,
                                                   BiConsumer<SerializationContextWrite, E> elementWriter,
                                                   Function<SerializationContextRead, E> elementReader) {
            this.type = type;
            this.clazz = clazz;
            this.elementWriter = elementWriter;
            this.elementReader = elementReader;
        }

        @Override
        public void write(SerializationContextWrite ctx, T target) {
            int length = getLength(target);
            ctx.writeInt(length);
            for (int i = 0; i < length; i++) {
                elementWriter.accept(ctx, getElement(target, i));
            }
        }

        @Override
        public T read(SerializationContextRead ctx, byte version) {
            int length = ctx.readInt();
            T array = createArray(length);
            for (int i = 0; i < length; i++) {
                setElement(array, i, elementReader.apply(ctx));
            }
            return array;
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
         * Returns the length of the array.
         *
         * @param array The array
         * @return The array length
         */
        private int getLength(T array) {
            return Array.getLength(array);
        }

        /**
         * Gets the element at the specified index.
         *
         * @param array The array
         * @param index The index
         * @return The element at the index
         */
        private E getElement(T array, int index) {
            @SuppressWarnings("unchecked")
            E element = (E) Array.get(array, index);
            return element;
        }

        /**
         * Creates a new array of the specified length.
         *
         * @param length The array length
         * @return The new array
         */
        protected abstract T createArray(int length);

        /**
         * Sets the element at the specified index.
         *
         * @param array The array
         * @param index The index
         * @param value The value to set
         */
        private void setElement(T array, int index, E value) {
            Array.set(array, index, value);
        }
    }

    /**
     * Serializer for {@code byte[]} arrays using bulk write/read operations.
     *
     * <p>
     * This serializer uses the optimized {@link SerializationContextWrite#writeBytes(byte[])} and
     * {@link SerializationContextRead#readBytes(int)} methods for efficient serialization.
     * </p>
     */
    private static final class ByteArraySerializer implements SingleBinarySerializer<byte[]> {
        static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

        private ByteArraySerializer() { }

        @Override
        public void write(SerializationContextWrite ctx, byte[] target) {
            ctx.writeInt(target.length);
            ctx.writeBytes(target);
        }

        @Override
        public byte[] read(SerializationContextRead ctx, byte version) {
            int length = ctx.readInt();
            return ctx.readBytes(length);
        }

        @Override
        public Class<byte[]> javaClass() {
            return byte[].class;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.BYTE_ARRAY_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /** Serializer for {@code short[]} arrays. */
    private static final class ShortArraySerializer extends AbstractPrimitiveArraySerializer<short[], Short> {
        static final ShortArraySerializer INSTANCE = new ShortArraySerializer();

        private ShortArraySerializer() {
            super(PrimitiveTypeIds.SHORT_ARRAY_TYPE_ID, short[].class,
                    SerializationContextWrite::writeShort, SerializationContextRead::readShort);
        }

        @Override
        protected short[] createArray(int length) {
            return new short[length];
        }
    }

    /** Serializer for {@code int[]} arrays. */
    private static final class IntArraySerializer extends AbstractPrimitiveArraySerializer<int[], Integer> {
        static final IntArraySerializer INSTANCE = new IntArraySerializer();

        private IntArraySerializer() {
            super(PrimitiveTypeIds.INT_ARRAY_TYPE_ID, int[].class,
                    SerializationContextWrite::writeInt, SerializationContextRead::readInt);
        }

        @Override
        protected int[] createArray(int length) {
            return new int[length];
        }
    }

    /** Serializer for {@code long[]} arrays. */
    private static final class LongArraySerializer extends AbstractPrimitiveArraySerializer<long[], Long> {
        static final LongArraySerializer INSTANCE = new LongArraySerializer();

        private LongArraySerializer() {
            super(PrimitiveTypeIds.LONG_ARRAY_TYPE_ID, long[].class,
                    SerializationContextWrite::writeLong, SerializationContextRead::readLong);
        }

        @Override
        protected long[] createArray(int length) {
            return new long[length];
        }
    }

    /** Serializer for {@code float[]} arrays. */
    private static final class FloatArraySerializer extends AbstractPrimitiveArraySerializer<float[], Float> {
        static final FloatArraySerializer INSTANCE = new FloatArraySerializer();

        private FloatArraySerializer() {
            super(PrimitiveTypeIds.FLOAT_ARRAY_TYPE_ID, float[].class,
                    SerializationContextWrite::writeFloat, SerializationContextRead::readFloat);
        }

        @Override
        protected float[] createArray(int length) {
            return new float[length];
        }
    }

    /** Serializer for {@code double[]} arrays. */
    private static final class DoubleArraySerializer extends AbstractPrimitiveArraySerializer<double[], Double> {
        static final DoubleArraySerializer INSTANCE = new DoubleArraySerializer();

        private DoubleArraySerializer() {
            super(PrimitiveTypeIds.DOUBLE_ARRAY_TYPE_ID, double[].class,
                    SerializationContextWrite::writeDouble, SerializationContextRead::readDouble);
        }

        @Override
        protected double[] createArray(int length) {
            return new double[length];
        }
    }

    /** Serializer for {@code boolean[]} arrays. */
    private static final class BooleanArraySerializer extends AbstractPrimitiveArraySerializer<boolean[], Boolean> {
        static final BooleanArraySerializer INSTANCE = new BooleanArraySerializer();

        private BooleanArraySerializer() {
            super(PrimitiveTypeIds.BOOLEAN_ARRAY_TYPE_ID, boolean[].class,
                    SerializationContextWrite::writeBoolean, SerializationContextRead::readBoolean);
        }

        @Override
        protected boolean[] createArray(int length) {
            return new boolean[length];
        }
    }

    /** Serializer for {@code char[]} arrays. */
    private static final class CharArraySerializer extends AbstractPrimitiveArraySerializer<char[], Character> {
        static final CharArraySerializer INSTANCE = new CharArraySerializer();

        private CharArraySerializer() {
            super(PrimitiveTypeIds.CHAR_ARRAY_TYPE_ID, char[].class,
                    SerializationContextWrite::writeShort, ctx -> (char) ctx.readShort());
        }

        @Override
        protected char[] createArray(int length) {
            return new char[length];
        }
    }
}
