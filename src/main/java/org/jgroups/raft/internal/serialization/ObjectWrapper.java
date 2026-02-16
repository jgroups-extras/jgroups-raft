package org.jgroups.raft.internal.serialization;

import java.io.IOException;

import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.WireType;

/**
 * A polymorphic container utilized to bridge generic or arbitrary types into the ProtoStream ecosystem.
 *
 * <p>
 * Because ProtoStream relies heavily on concrete type definitions and schemas, serializing polymorphic or abstract structures
 * (such as storing an arbitrary object inside a Map) requires a specialized envelope. {@code ObjectWrapper} fulfills this
 * role by encapsulating the generic object, serializing it into an opaque byte array, and storing it safely within the ProtoStream schema.
 * </p>
 *
 * <p>
 * This is inspired by the MarshallableObject class available in Infinispan.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @param <T> The type of the encapsulated object.
 */
@ProtoTypeId(ProtoStreamTypes.WRAPPED_OBJECT)
public class ObjectWrapper<T> {

    private final T object;

    @ProtoFactory
    ObjectWrapper() {
        throw new IllegalStateException("This constructor should never be called");
    }

    private ObjectWrapper(T object) {
        this.object = object;
    }

    /**
     * Wraps an object inside a new ObjectWrapper instance.
     *
     * @param object The object to wrap.
     * @param <T>    The type of the object.
     * @return A new ObjectWrapper containing the provided object.
     */
    public static <T> ObjectWrapper<T> create(T object) {
        return new ObjectWrapper<>(object);
    }

    /**
     * Extracts the inner object from an ObjectWrapper instance.
     *
     * @param wrapper The wrapper to extract from.
     * @param <T>     The expected type of the inner object.
     * @return The unwrapped object, or null if the wrapper is null.
     */
    public static <T> T unwrap(ObjectWrapper<T> wrapper) {
        return wrapper == null ? null : wrapper.get();
    }

    private T get() {
        return object;
    }

    /**
     * A custom ProtoStream marshaller designed to bypass automatic schema generation for the wrapper.
     *
     * <p>
     * This marshaller intercepts the Protobuf encoding process at the tag level. It manually converts the inner object
     * to bytes using the primary {@link Serializer}, and embeds it directly into the Protobuf stream as a length-delimited
     * byte array field.
     * </p>
     */
    @SuppressWarnings("rawtypes")
    public static class Marshaller implements ProtobufTagMarshaller<ObjectWrapper> {

        private final String typeName;
        private final Serializer serializer;

        public Marshaller(String typeName, Serializer serializer) {
            this.typeName = typeName;
            this.serializer = serializer;
        }

        @Override
        public ObjectWrapper read(ReadContext ctx) throws IOException {
            TagReader in = ctx.getReader();
            byte[] bytes = null;
            boolean done = false;
            while (!done) {
                final int tag = in.readTag();
                switch (tag) {
                    // end of message
                    case 0:
                        done = true;
                        break;
                    // field number 1
                    case 1 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
                        bytes = in.readByteArray();
                        break;
                    }
                    default: {
                        if (!in.skipField(tag)) done = true;
                    }
                }
            }
            Object userObject = serializer.deserialize(bytes);
            return new ObjectWrapper<>(userObject);
        }

        @Override
        public void write(WriteContext ctx, ObjectWrapper wrapper) throws IOException {
            Object userObject = wrapper.get();
            byte[] bytes = serializer.serialize(userObject);
            // field number 1
            ctx.getWriter().writeBytes(1, bytes);
        }

        @Override
        public Class<? extends ObjectWrapper> getJavaClass() {
            return ObjectWrapper.class;
        }

        @Override
        public String getTypeName() {
            return typeName;
        }
    }
}
