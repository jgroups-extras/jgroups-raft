package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.util.io.CustomByteBuffer;
import org.jgroups.util.ByteArrayDataOutputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.TreeMap;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link ByteArrayDataOutputSerializationContextWrite} to ensure wire format
 * compatibility with {@link DefaultSerializationContext}.
 *
 * <p>
 * This test verifies that the zero-copy streaming serialization path produces
 * <b>exactly the same byte output</b> as the buffer-based path. This is critical for
 * correctness - both paths must produce identical wire formats.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ByteArrayDataOutputSerializationContextWriteTest {

    private BinarySerializationRegistry registry;

    @BeforeMethod
    public void setUp() {
        registry = new BinarySerializationRegistry();
        BinarySerializer.registerInternalSerializers(registry);
        BinarySerializer.registerBuiltInSerializers(registry);
    }

    // ========== Wire Format Compatibility Tests ==========

    @Test
    public void testNullObjectProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer(null);
        byte[] streamBytes = serializeWithStream(null);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testIntegerProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer(42);
        byte[] streamBytes = serializeWithStream(42);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testStringProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer("Hello, World!");
        byte[] streamBytes = serializeWithStream("Hello, World!");

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testByteArrayProducesSameBytes() {
        byte[] data = {1, 2, 3, 4, 5};
        byte[] bufferBytes = serializeWithBuffer(data);
        byte[] streamBytes = serializeWithStream(data);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testLongProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer(123456789012345L);
        byte[] streamBytes = serializeWithStream(123456789012345L);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testDoubleProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer(3.141592653589793);
        byte[] streamBytes = serializeWithStream(3.141592653589793);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testBooleanProducesSameBytes() {
        byte[] bufferBytesTrue = serializeWithBuffer(true);
        byte[] streamBytesTrue = serializeWithStream(true);
        assertThat(streamBytesTrue).isEqualTo(bufferBytesTrue);

        byte[] bufferBytesFalse = serializeWithBuffer(false);
        byte[] streamBytesFalse = serializeWithStream(false);
        assertThat(streamBytesFalse).isEqualTo(bufferBytesFalse);
    }

    @Test
    public void testCharacterProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer('Z');
        byte[] streamBytes = serializeWithStream('Z');

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testFloatProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer(2.71828f);
        byte[] streamBytes = serializeWithStream(2.71828f);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testShortProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer((short) 12345);
        byte[] streamBytes = serializeWithStream((short) 12345);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testByteProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer((byte) 127);
        byte[] streamBytes = serializeWithStream((byte) 127);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    // ========== Complex Object Tests ==========

    @Test
    public void testHashMapProducesSameBytes() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        byte[] bufferBytes = serializeWithBuffer(map);
        byte[] streamBytes = serializeWithStream(map);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testArrayListProducesSameBytes() {
        ArrayList<String> list = new ArrayList<>();
        list.add("first");
        list.add("second");
        list.add("third");

        byte[] bufferBytes = serializeWithBuffer(list);
        byte[] streamBytes = serializeWithStream(list);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testNestedMapProducesSameBytes() {
        HashMap<String, HashMap<String, Integer>> outer = new HashMap<>();
        HashMap<String, Integer> inner1 = new HashMap<>();
        inner1.put("a", 1);
        inner1.put("b", 2);
        HashMap<String, Integer> inner2 = new HashMap<>();
        inner2.put("x", 10);
        inner2.put("y", 20);

        outer.put("first", inner1);
        outer.put("second", inner2);

        byte[] bufferBytes = serializeWithBuffer(outer);
        byte[] streamBytes = serializeWithStream(outer);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testEmptyCollectionProducesSameBytes() {
        ArrayList<String> emptyList = new ArrayList<>();

        byte[] bufferBytes = serializeWithBuffer(emptyList);
        byte[] streamBytes = serializeWithStream(emptyList);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testMapWithNullValuesProducesSameBytes() {
        HashMap<String, String> map = new HashMap<>();
        map.put("key1", null);
        map.put("key2", "value");
        map.put(null, "null-key");

        byte[] bufferBytes = serializeWithBuffer(map);
        byte[] streamBytes = serializeWithStream(map);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testLargeByteArrayProducesSameBytes() {
        byte[] largeArray = new byte[10000];
        new Random(42).nextBytes(largeArray);

        byte[] bufferBytes = serializeWithBuffer(largeArray);
        byte[] streamBytes = serializeWithStream(largeArray);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testLongStringProducesSameBytes() {
        String longString = "This is a long string with repeated content. ".repeat(500);

        byte[] bufferBytes = serializeWithBuffer(longString);
        byte[] streamBytes = serializeWithStream(longString);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testIntArrayProducesSameBytes() {
        int[] array = {1, 2, 3, 4, 5, 100, 200, 300};

        byte[] bufferBytes = serializeWithBuffer(array);
        byte[] streamBytes = serializeWithStream(array);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testLongArrayProducesSameBytes() {
        long[] array = {1L, 2L, 3L, 100000000000L};

        byte[] bufferBytes = serializeWithBuffer(array);
        byte[] streamBytes = serializeWithStream(array);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testTreeMapProducesSameBytes() {
        TreeMap<String, Integer> map = new TreeMap<>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 3);

        byte[] bufferBytes = serializeWithBuffer(map);
        byte[] streamBytes = serializeWithStream(map);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testLinkedHashMapProducesSameBytes() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("first", "1st");
        map.put("second", "2nd");
        map.put("third", "3rd");

        byte[] bufferBytes = serializeWithBuffer(map);
        byte[] streamBytes = serializeWithStream(map);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testHashSetProducesSameBytes() {
        HashSet<String> set = new HashSet<>();
        set.add("apple");
        set.add("banana");
        set.add("cherry");

        byte[] bufferBytes = serializeWithBuffer(set);
        byte[] streamBytes = serializeWithStream(set);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    // ========== Edge Cases ==========

    @Test
    public void testZeroLengthStringProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer("");
        byte[] streamBytes = serializeWithStream("");

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testZeroLengthByteArrayProducesSameBytes() {
        byte[] bufferBytes = serializeWithBuffer(new byte[0]);
        byte[] streamBytes = serializeWithStream(new byte[0]);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testMixedTypeCollectionProducesSameBytes() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(42);
        list.add("string");
        list.add(3.14);
        list.add(true);
        list.add(null);

        byte[] bufferBytes = serializeWithBuffer(list);
        byte[] streamBytes = serializeWithStream(list);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    @Test
    public void testDeeplyNestedStructureProducesSameBytes() {
        // Create nested maps: map -> map -> list -> map
        HashMap<String, Object> level1 = new HashMap<>();
        HashMap<String, Object> level2 = new HashMap<>();
        ArrayList<Object> level3 = new ArrayList<>();
        HashMap<String, Integer> level4 = new HashMap<>();

        level4.put("deep", 123);
        level3.add(level4);
        level3.add("item");
        level2.put("nested", level3);
        level1.put("root", level2);

        byte[] bufferBytes = serializeWithBuffer(level1);
        byte[] streamBytes = serializeWithStream(level1);

        assertThat(streamBytes).isEqualTo(bufferBytes);
    }

    // ========== Deserialization Tests ==========

    @Test
    public void testStreamSerializedDataCanBeDeserialized() {
        HashMap<String, Integer> original = new HashMap<>();
        original.put("one", 1);
        original.put("two", 2);

        byte[] streamBytes = serializeWithStream(original);

        // Deserialize using standard path
        Serializer serializer = Serializer.create(SerializationRegistry.create());
        @SuppressWarnings("unchecked")
        HashMap<String, Integer> deserialized = serializer.deserialize(streamBytes, HashMap.class);

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get("one")).isEqualTo(1);
        assertThat(deserialized.get("two")).isEqualTo(2);
    }

    @Test
    public void testBufferSerializedDataCanBeDeserialized() {
        ArrayList<String> original = new ArrayList<>();
        original.add("a");
        original.add("b");
        original.add("c");

        byte[] bufferBytes = serializeWithBuffer(original);

        // Deserialize using standard path
        Serializer serializer = Serializer.create(SerializationRegistry.create());
        @SuppressWarnings("unchecked")
        ArrayList<String> deserialized = serializer.deserialize(bufferBytes, ArrayList.class);

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized).containsExactly("a", "b", "c");
    }

    // ========== Helper Methods ==========

    /**
     * Serializes an object using the buffer-based path (DefaultSerializationContext).
     */
    private byte[] serializeWithBuffer(Object obj) {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(1024);
        DefaultSerializationContext ctx = new DefaultSerializationContext(registry, buffer);
        ctx.writeObject(obj);
        return buffer.toByteArray();
    }

    /**
     * Serializes an object using the stream-based path (ByteArrayDataOutputSerializationContextWrite).
     */
    private byte[] serializeWithStream(Object obj) {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(1024);
        ByteArrayDataOutputSerializationContextWrite ctx =
                new ByteArrayDataOutputSerializationContextWrite(registry, baos);
        ctx.writeObject(obj);

        // Extract the written bytes
        byte[] result = new byte[baos.position()];
        System.arraycopy(baos.buffer(), 0, result, 0, baos.position());
        return result;
    }
}
