package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.Serializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for mutable map serialization with {@link BinarySerializer}.
 *
 * <p>
 * This test class validates serialization and deserialization for common mutable map types,
 * including hash-based and sorted maps. Tests verify proper handling of empty maps, maps with
 * primitives (auto-boxed), nested maps, and maps with null keys/values.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class MapSerializerTest {

    private Serializer serializer;

    @BeforeMethod
    public void setUp() {
        SerializationRegistry registry = SerializationRegistry.create();
        serializer = Serializer.create(registry);
    }

    // ========== HashMap Tests ==========

    @Test
    public void testHashMapWithStringKeysAndValues() {
        HashMap<String, String> original = new HashMap<>();
        original.put("key1", "value1");
        original.put("key2", "value2");
        original.put("key3", "value3");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, String> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.get("key1")).isEqualTo("value1");
        assertThat(deserialized.get("key2")).isEqualTo("value2");
        assertThat(deserialized.get("key3")).isEqualTo("value3");
    }

    @Test
    public void testHashMapWithIntegerKeysAndValues() {
        HashMap<Integer, Integer> original = new HashMap<>();
        original.put(1, 100);
        original.put(2, 200);
        original.put(3, 300);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<Integer, Integer> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.get(1)).isEqualTo(100);
        assertThat(deserialized.get(2)).isEqualTo(200);
        assertThat(deserialized.get(3)).isEqualTo(300);
    }

    @Test
    public void testEmptyHashMap() {
        HashMap<String, String> original = new HashMap<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, String> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testHashMapWithNullKey() {
        HashMap<String, String> original = new HashMap<>();
        original.put(null, "null-key-value");
        original.put("key1", "value1");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, String> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get(null)).isEqualTo("null-key-value");
        assertThat(deserialized.get("key1")).isEqualTo("value1");
    }

    @Test
    public void testHashMapWithNullValue() {
        HashMap<String, String> original = new HashMap<>();
        original.put("key1", null);
        original.put("key2", "value2");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, String> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get("key1")).isNull();
        assertThat(deserialized.get("key2")).isEqualTo("value2");
    }

    @Test
    public void testHashMapWithNullKeyAndValue() {
        HashMap<String, String> original = new HashMap<>();
        original.put(null, null);
        original.put("key1", "value1");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, String> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get(null)).isNull();
        assertThat(deserialized.get("key1")).isEqualTo("value1");
    }

    @Test
    public void testHashMapNested() {
        HashMap<String, Map<String, Integer>> original = new HashMap<>();
        original.put("map1", Map.of("a", 1, "b", 2));
        original.put("map2", Map.of("c", 3, "d", 4));

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, Map<String, Integer>> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get("map1")).containsEntry("a", 1).containsEntry("b", 2);
        assertThat(deserialized.get("map2")).containsEntry("c", 3).containsEntry("d", 4);
    }

    @Test
    public void testLargeHashMap() {
        HashMap<Integer, String> original = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            original.put(i, "value-" + i);
        }

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<Integer, String> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(1000);
        assertThat(deserialized).isEqualTo(original);
    }

    // ========== LinkedHashMap Tests ==========

    @Test
    public void testLinkedHashMapMaintainsInsertionOrder() {
        LinkedHashMap<String, Integer> original = new LinkedHashMap<>();
        original.put("first", 1);
        original.put("second", 2);
        original.put("third", 3);
        original.put("fourth", 4);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, Integer> deserialized = serializer.deserialize(bytes, LinkedHashMap.class);

        assertThat(deserialized).hasSize(4);
        assertThat(deserialized.keySet()).containsExactly("first", "second", "third", "fourth");
        assertThat(deserialized.values()).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void testEmptyLinkedHashMap() {
        LinkedHashMap<String, String> original = new LinkedHashMap<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, String> deserialized = serializer.deserialize(bytes, LinkedHashMap.class);

        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testLinkedHashMapWithPrimitives() {
        LinkedHashMap<Long, Double> original = new LinkedHashMap<>();
        original.put(1L, 1.1);
        original.put(2L, 2.2);
        original.put(3L, 3.3);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedHashMap<Long, Double> deserialized = serializer.deserialize(bytes, LinkedHashMap.class);

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.keySet()).containsExactly(1L, 2L, 3L);
        assertThat(deserialized.get(1L)).isEqualTo(1.1);
        assertThat(deserialized.get(2L)).isEqualTo(2.2);
        assertThat(deserialized.get(3L)).isEqualTo(3.3);
    }

    // ========== TreeMap Tests ==========

    @Test
    public void testTreeMapMaintainsSortedOrder() {
        TreeMap<Integer, String> original = new TreeMap<>();
        original.put(30, "thirty");
        original.put(10, "ten");
        original.put(20, "twenty");
        original.put(50, "fifty");
        original.put(40, "forty");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        TreeMap<Integer, String> deserialized = serializer.deserialize(bytes, TreeMap.class);

        assertThat(deserialized).hasSize(5);
        assertThat(deserialized.keySet()).containsExactly(10, 20, 30, 40, 50);
        assertThat(deserialized.values()).containsExactly("ten", "twenty", "thirty", "forty", "fifty");
    }

    @Test
    public void testTreeMapWithStringKeys() {
        TreeMap<String, Integer> original = new TreeMap<>();
        original.put("zebra", 4);
        original.put("apple", 1);
        original.put("monkey", 3);
        original.put("banana", 2);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        TreeMap<String, Integer> deserialized = serializer.deserialize(bytes, TreeMap.class);

        assertThat(deserialized).hasSize(4);
        assertThat(deserialized.keySet()).containsExactly("apple", "banana", "monkey", "zebra");
    }

    @Test
    public void testEmptyTreeMap() {
        TreeMap<String, String> original = new TreeMap<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        TreeMap<String, String> deserialized = serializer.deserialize(bytes, TreeMap.class);

        assertThat(deserialized).isEmpty();
    }

    // ========== Mixed Type Tests ==========

    @Test
    public void testHashMapWithMixedValueTypes() {
        HashMap<String, Number> original = new HashMap<>();
        original.put("int", 1);
        original.put("long", 2L);
        original.put("float", 3.0f);
        original.put("double", 4.0);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, Number> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(4);
        assertThat(deserialized.get("int")).isEqualTo(1);
        assertThat(deserialized.get("long")).isEqualTo(2L);
        assertThat(deserialized.get("float")).isEqualTo(3.0f);
        assertThat(deserialized.get("double")).isEqualTo(4.0);
    }

    @Test
    public void testHashMapStringToList() {
        HashMap<String, java.util.List<Integer>> original = new HashMap<>();
        original.put("list1", java.util.List.of(1, 2, 3));
        original.put("list2", java.util.List.of(4, 5, 6));

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashMap<String, java.util.List<Integer>> deserialized = serializer.deserialize(bytes, HashMap.class);

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get("list1")).containsExactly(1, 2, 3);
        assertThat(deserialized.get("list2")).containsExactly(4, 5, 6);
    }

    // ========== Null Handling ==========

    @Test
    public void testNullHashMap() {
        byte[] bytes = serializer.serialize(null);
        HashMap<?, ?> deserialized = serializer.deserialize(bytes, HashMap.class);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testNullLinkedHashMap() {
        byte[] bytes = serializer.serialize(null);
        LinkedHashMap<?, ?> deserialized = serializer.deserialize(bytes, LinkedHashMap.class);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testNullTreeMap() {
        byte[] bytes = serializer.serialize(null);
        TreeMap<?, ?> deserialized = serializer.deserialize(bytes, TreeMap.class);
        assertThat(deserialized).isNull();
    }
}
