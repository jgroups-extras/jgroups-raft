package org.jgroups.raft.internal.registry;

import org.jgroups.raft.JGroupsRaftCustomMarshaller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.jcip.annotations.GuardedBy;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.WireType;

public sealed interface SerializationRegistry permits SerializationRegistry.DefaultSerializationRegistry {

    void register(JGroupsRaftCustomMarshaller<?> marshaller);

    void register(BaseMarshaller<?> marshaller);

    void register(SerializationContextInitializer sci);

    ImmutableSerializationContext context();

    static SerializationRegistry create() {
        return new DefaultSerializationRegistry();
    }

    final class DefaultSerializationRegistry implements SerializationRegistry {

        private final SerializerContext global = new SerializerContext();

        private DefaultSerializationRegistry() { }

        @Override
        public void register(JGroupsRaftCustomMarshaller<?> marshaller) {
            global.register(marshaller);
        }

        @Override
        public void register(BaseMarshaller<?> marshaller) {
            global.register(marshaller);
        }

        @Override
        public void register(SerializationContextInitializer sci) {
            global.register(sci);
        }

        @Override
        public ImmutableSerializationContext context() {
            return global.ctx;
        }

        private static void register(SerializationContextInitializer sci, SerializationContext ctx) {
            sci.registerSchema(ctx);
            sci.registerMarshallers(ctx);
        }

        private static final class SerializerContext {
            private final List<BaseMarshaller<?>> marshallers = new ArrayList<>();
            private final SerializationContext ctx = ProtobufUtil.newSerializationContext(Configuration.builder()
                    .wrapCollectionElements(true)
                    .build());

            void register(SerializationContextInitializer sci) {
                DefaultSerializationRegistry.register(sci, ctx);
                marshallers.forEach(ctx::registerMarshaller);
            }

            void register(JGroupsRaftCustomMarshaller<?> delegate) {
                MarshallerWrapper<?> wrapper = new MarshallerWrapper<>(delegate);
                register(wrapper);
            }

            void register(BaseMarshaller<?> marshaller) {
                marshallers.add(marshaller);
                ctx.registerMarshaller(marshaller);
            }
        }

        private record MarshallerWrapper<T>(@GuardedBy("this") JGroupsRaftCustomMarshaller<T> delegate) implements ProtobufTagMarshaller<T> {

            @Override
            public T read(ReadContext ctx) throws IOException {
                TagReader in = ctx.getReader();
                byte[] bytes = null;
                boolean done = false;
                while (!done) {
                    final int tag = in.readTag();
                    switch (tag) {
                        case 0:
                            done = true;
                            break;

                        case 1 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED:
                            bytes = in.readByteArray();
                            break;

                        default:
                            if (!in.skipField(tag))
                                done = true;
                            break;
                    }
                }

                synchronized (this) {
                    return delegate.read(bytes);
                }
            }

            @Override
            public void write(WriteContext ctx, T t) throws IOException {
                byte[] datum;
                synchronized (this) {
                    datum = delegate.write(t);
                }
                ctx.getWriter().writeBytes(1, datum);
            }

            @Override
            public synchronized Class<? extends T> getJavaClass() {
                return delegate.javaClass();
            }

            @Override
            public String getTypeName() {
                return WrappedMessage.PROTOBUF_TYPE_NAME;
            }
        }
    }
}
