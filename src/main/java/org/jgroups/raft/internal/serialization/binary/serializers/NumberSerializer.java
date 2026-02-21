package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Internal serializers for Java primitive wrapper types.
 *
 * <p>
 * This class provides serializers for the following types:
 * </p>
 * <ul>
 *   <li>{@link Byte}</li>
 *   <li>{@link Short}</li>
 *   <li>{@link Integer}</li>
 *   <li>{@link Long}</li>
 *   <li>{@link Float}</li>
 *   <li>{@link Double}</li>
 *   <li>{@link Boolean}</li>
 *   <li>{@link Character}</li>
 * </ul>
 *
 * <p>
 * These serializers are registered in the serialization registry but are bypassed by the fast-path optimization
 * in {@code DefaultSerializationContext}. They exist for completeness and in case direct registry access is needed.
 * </p>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class NumberSerializer {

    static final SingleBinarySerializer<?>[] SERIALIZERS = {
            ByteSerializer.INSTANCE,
            ShortSerializer.INSTANCE,
            IntegerSerializer.INSTANCE,
            LongSerializer.INSTANCE,
            FloatSerializer.INSTANCE,
            DoubleSerializer.INSTANCE,
            BooleanSerializer.INSTANCE,
            CharacterSerializer.INSTANCE
    };

    private NumberSerializer() { }

    /**
     * Base class for all number serializers using function composition.
     *
     * @param <T> The number type
     */
    private static class AbstractNumberSerializer<T extends Number> implements SingleBinarySerializer<T> {

        private final int type;
        private final BiConsumer<SerializationContextWrite, T> writer;
        private final Function<SerializationContextRead, T> reader;
        private final Class<T> clazz;

        private AbstractNumberSerializer(int type, BiConsumer<SerializationContextWrite, T> writer, Function<SerializationContextRead, T> reader, Class<T> clazz) {
            this.type = type;
            this.writer = writer;
            this.reader = reader;
            this.clazz = clazz;
        }

        @Override
        public void write(SerializationContextWrite ctx, T target) {
            writer.accept(ctx, target);
        }

        @Override
        public T read(SerializationContextRead ctx, byte version) {
            return reader.apply(ctx);
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
    }

    private static final class ByteSerializer extends AbstractNumberSerializer<Byte> {
        static final ByteSerializer INSTANCE = new ByteSerializer();

        private ByteSerializer() {
            super(PrimitiveTypeIds.BYTE_TYPE_ID, SerializationContextWrite::writeByte, SerializationContextRead::readByte, Byte.class);
        }
    }

    private static final class ShortSerializer extends AbstractNumberSerializer<Short> {
        static final ShortSerializer INSTANCE = new ShortSerializer();

        private ShortSerializer() {
            super(PrimitiveTypeIds.SHORT_TYPE_ID, SerializationContextWrite::writeShort, SerializationContextRead::readShort, Short.class);
        }
    }

    private static final class IntegerSerializer extends AbstractNumberSerializer<Integer> {
        static final IntegerSerializer INSTANCE = new IntegerSerializer();

        private IntegerSerializer() {
            super(PrimitiveTypeIds.INTEGER_TYPE_ID, SerializationContextWrite::writeInt, SerializationContextRead::readInt, Integer.class);
        }
    }

    private static final class LongSerializer extends AbstractNumberSerializer<Long> {
        static final LongSerializer INSTANCE = new LongSerializer();

        private LongSerializer() {
            super(PrimitiveTypeIds.LONG_TYPE_ID, SerializationContextWrite::writeLong, SerializationContextRead::readLong, Long.class);
        }
    }

    private static final class FloatSerializer extends AbstractNumberSerializer<Float> {
        static final FloatSerializer INSTANCE = new FloatSerializer();

        private FloatSerializer() {
            super(PrimitiveTypeIds.FLOAT_TYPE_ID, SerializationContextWrite::writeFloat, SerializationContextRead::readFloat, Float.class);
        }
    }

    private static final class DoubleSerializer extends AbstractNumberSerializer<Double> {
        static final DoubleSerializer INSTANCE = new DoubleSerializer();

        private DoubleSerializer() {
            super(PrimitiveTypeIds.DOUBLE_TYPE_ID, SerializationContextWrite::writeDouble, SerializationContextRead::readDouble, Double.class);
        }
    }

    /**
     * Boolean serializer.
     */
    private static class BooleanSerializer implements SingleBinarySerializer<Boolean> {
        static final BooleanSerializer INSTANCE = new BooleanSerializer();

        private BooleanSerializer() { }

        @Override
        public void write(SerializationContextWrite ctx, Boolean target) {
            ctx.writeBoolean(target);
        }

        @Override
        public Boolean read(SerializationContextRead ctx, byte version) {
            return ctx.readBoolean();
        }

        @Override
        public Class<Boolean> javaClass() {
            return Boolean.class;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.BOOLEAN_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }

    /**
     * Character serializer.
     */
    private static class CharacterSerializer implements SingleBinarySerializer<Character> {
        static final CharacterSerializer INSTANCE = new CharacterSerializer();

        private CharacterSerializer() { }

        @Override
        public void write(SerializationContextWrite ctx, Character target) {
            ctx.writeShort(target);
        }

        @Override
        public Character read(SerializationContextRead ctx, byte version) {
            return (char) ctx.readShort();
        }

        @Override
        public Class<Character> javaClass() {
            return Character.class;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.CHARACTER_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
