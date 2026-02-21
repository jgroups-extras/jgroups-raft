package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

/**
 * Internal serializers for character sequence types.
 *
 * <p>
 * This class provides serializers for {@link String} using UTF-8 encoding with length prefixing. The serialization format
 * is compatible with {@code java.io.DataOutput.writeUTF} and {@code java.io.DataInput.readUTF}, supporting strings up to
 * 32KB in encoded length.
 * </p>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class CharSequenceSerializer {

    static final SingleBinarySerializer<?>[] SERIALIZERS = new SingleBinarySerializer<?>[]{
            StringSerializer.INSTANCE,
    };

    private CharSequenceSerializer() { }

    /**
     * Serializer for {@link String} values using modified UTF-8 encoding.
     *
     * <p>
     * Strings are written with a 2-byte length prefix followed by the UTF-8 encoded bytes. Maximum string length is 32KB
     * when encoded.
     * </p>
     */
    private static final class StringSerializer implements SingleBinarySerializer<String> {
        private static final StringSerializer INSTANCE = new StringSerializer();

        @Override
        public void write(SerializationContextWrite ctx, String target) {
            ctx.writeUTF(target);
        }

        @Override
        public String read(SerializationContextRead ctx, byte version) {
            return ctx.readUTF();
        }

        @Override
        public Class<String> javaClass() {
            return String.class;
        }

        @Override
        public int type() {
            return PrimitiveTypeIds.STRING_TYPE_ID;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
