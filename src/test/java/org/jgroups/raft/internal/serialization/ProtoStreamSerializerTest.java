package org.jgroups.raft.internal.serialization;

import org.jgroups.Global;
import org.jgroups.raft.JGroupsRaftCustomMarshaller;
import org.jgroups.raft.internal.registry.SerializationRegistry;
import org.jgroups.raft.tests.serialization.TestDataHolderCustom;
import org.jgroups.raft.tests.serialization.TestDataHolderProto;
import org.jgroups.raft.tests.serialization.TestSerializationInitializerImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ProtoStreamSerializerTest {

    private Serializer serializer;

    @BeforeClass
    protected void beforeClass() {
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        serializer = Serializer.protoStream(registry);
    }

    @Test(dataProvider = "objects")
    public void testSimpleSerialization(Object object) {
        assertSerialization(object);
    }

    public void testSerializingObjects() {
        TestDataHolderProto holder = new TestDataHolderProto("hello", 0, 42, new byte[16]);
        assertSerialization(holder);
    }

    public void testCustomSerializer() {
        JGroupsRaftCustomMarshaller<TestDataHolderCustom> customMarshaller = new JGroupsRaftCustomMarshaller<>() {
            @Override
            public Class<? extends TestDataHolderCustom> javaClass() {
                return TestDataHolderCustom.class;
            }

            @Override
            public byte[] write(TestDataHolderCustom obj) {
                ByteBuffer buffer = ByteBuffer.allocate(512);

                byte[] id = obj.id().getBytes(StandardCharsets.UTF_8);
                buffer.putInt(id.length);
                buffer.put(id);
                buffer.putLong(obj.index());
                buffer.putInt(obj.term());

                buffer.putInt(obj.data().length);
                buffer.put(obj.data());
                buffer.flip();
                return buffer.array();
            }

            @Override
            public TestDataHolderCustom read(byte[] datum) {
                ByteBuffer buffer = ByteBuffer.wrap(datum);
                byte[] id = new byte[buffer.getInt()];
                buffer.get(id);

                long index = buffer.getLong();
                int term = buffer.getInt();

                byte[] data = new byte[buffer.getInt()];
                buffer.get(data);

                return new TestDataHolderCustom(new String(id, StandardCharsets.UTF_8), index, term, data);
            }
        };
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(customMarshaller);

        Serializer s = Serializer.protoStream(registry);

        TestDataHolderCustom holder = new TestDataHolderCustom("hello world", 10, 48, new byte[32]);
        byte[] datum = s.serialize(holder);
        TestDataHolderCustom actual = s.deserialize(datum);

        assertThat(actual).isEqualTo(holder);
    }

    private <T> void assertSerialization(T obj) {
        byte[] datum = serializer.serialize(obj);
        T actual = serializer.deserialize(datum);
        assertThat(actual).isEqualTo(obj);
    }

    @DataProvider
    static Object[][] objects() {
        return new Object[][] {
                { UUID.class },
                { null },
                { 1 },
                { 42L },
                { "String" },
                { 3.1415F },
                { 3.1415D },
                { UUID.randomUUID() },
                { new byte[] { 1, 2, 3, 4 } },
        };
    }
}
