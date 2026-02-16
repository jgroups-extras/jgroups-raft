package org.jgroups.raft.internal.serialization;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.JGroupsRaftCustomMarshaller;
import org.jgroups.raft.internal.registry.SerializationRegistry;
import org.jgroups.raft.serialization.TestDataHolderCustom;
import org.jgroups.raft.serialization.TestDataHolderProto;
import org.jgroups.raft.serialization.TestSerializationInitializerImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

    @Test
    public void testUnregisteredClassThrowsException() {
        // Create an arbitrary class that has no ProtoStream schema or Custom Marshaller
        class UnregisteredDummy {
            private final String data = "secret";
        }

        UnregisteredDummy unregistered = new UnregisteredDummy();

        assertThatThrownBy(() -> serializer.serialize(unregistered))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not serializable.");
    }

    @Test
    public void testDeserializeNullAndEmptyBuffers() {
        Object fromNull = serializer.deserialize(null);
        assertThat(fromNull).isNull();

        Object fromEmpty = serializer.deserialize(new byte[0]);
        assertThat(fromEmpty).isNull();

        Object toNull = serializer.deserialize(null);
        assertThat(toNull).isNull();
    }

    @Test
    public void testCollectionsSerialization() {
        List<String> list = new ArrayList<>(List.of("node1", "node2", "node3"));
        assertSerialization(list);

        // See ProtoStream#514
//        Map<String, Integer> map = Map.of("term", 5, "index", 100);
//        assertSerialization(map);

        Set<Double> set = new HashSet<>(Set.of(1.5, 2.5, 3.5));
        assertSerialization(set);
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
                { true },
                { (byte) 0x0A },
                { (short) 256 },
                { 'R' },
                { java.time.Instant.now() },
                { new java.util.Date() },
                { UUID.randomUUID() },
                { new byte[] { 1, 2, 3, 4 } },
        };
    }
}
