package org.jgroups.raft.internal.serialization;

import java.io.IOException;

import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.WireType;

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

    public static <T> ObjectWrapper<T> create(T object) {
        return new ObjectWrapper<>(object);
    }

    public static <T> T unwrap(ObjectWrapper<T> wrapper) {
        return wrapper == null ? null : wrapper.get();
    }

    private T get() {
        return object;
    }

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
