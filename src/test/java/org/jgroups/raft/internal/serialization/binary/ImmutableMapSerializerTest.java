package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.Serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for immutable map serialization with {@link BinarySerializer}.
 *
 * <p>
 * This test class validates serialization and deserialization for JVM-provided immutable maps,
 * ensuring that immutability is preserved across serialization boundaries. Tests cover empty maps,
 * singleton maps, and maps created via {@link Map#of(Object, Object, ...)}.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ImmutableMapSerializerTest {

    private Serializer serializer;

    @BeforeMethod
    public void setUp() {
        SerializationRegistry registry = SerializationRegistry.create();
        serializer = Serializer.create(registry);
    }

    // ========== Collections.emptyMap() Tests ==========

    @Test
    public void testEmptyMapRoundTrip() {
        Map<String, String> original = Collections.emptyMap();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
        assertThat(deserialized).isEqualTo(Collections.emptyMap());
    }

    @Test
    public void testEmptyMapIsImmutable() {
        Map<String, String> original = Collections.emptyMap();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.put("key", "value"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ========== Collections.singletonMap() Tests ==========

    @Test
    public void testSingletonMapWithStrings() {
        Map<String, String> original = Collections.singletonMap("key", "value");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(1);
        assertThat(deserialized.get("key")).isEqualTo("value");
    }

    @Test
    public void testSingletonMapWithIntegers() {
        Map<Integer, Integer> original = Collections.singletonMap(42, 100);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(1);
        assertThat(deserialized.get(42)).isEqualTo(100);
    }

    @Test
    public void testSingletonMapWithNullKey() {
        Map<String, String> original = Collections.singletonMap(null, "null-key-value");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(1);
        assertThat(deserialized.get(null)).isEqualTo("null-key-value");
    }

    @Test
    public void testSingletonMapWithNullValue() {
        Map<String, String> original = Collections.singletonMap("key", null);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(1);
        assertThat(deserialized.get("key")).isNull();
    }

    @Test
    public void testSingletonMapIsImmutable() {
        Map<String, String> original = Collections.singletonMap("key", "value");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.put("another", "value"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ========== Collections.unmodifiableMap() Tests ==========

    @Test
    public void testUnmodifiableMapRoundTrip() {
        Map<String, Integer> mutableMap = new HashMap<>();
        mutableMap.put("one", 1);
        mutableMap.put("two", 2);
        mutableMap.put("three", 3);
        Map<String, Integer> original = Collections.unmodifiableMap(mutableMap);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.get("one")).isEqualTo(1);
        assertThat(deserialized.get("two")).isEqualTo(2);
        assertThat(deserialized.get("three")).isEqualTo(3);
    }

    @Test
    public void testUnmodifiableMapIsImmutable() {
        Map<String, String> original = Collections.unmodifiableMap(Map.of("key", "value"));

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.put("another", "value"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testUnmodifiableMapEmpty() {
        Map<String, String> original = Collections.unmodifiableMap(new HashMap<>());

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
    }

    // ========== Map.of() Tests ==========

    @Test
    public void testMapOfEmpty() {
        Map<String, String> original = Map.of();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
        assertThat(deserialized).isEqualTo(Map.of());
    }

    @Test
    public void testMapOfOneEntry() {
        Map<String, String> original = Map.of("key1", "value1");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(1);
        assertThat(deserialized.get("key1")).isEqualTo("value1");
    }

    @Test
    public void testMapOfTwoEntries() {
        Map<String, String> original = Map.of("key1", "value1", "key2", "value2");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get("key1")).isEqualTo("value1");
        assertThat(deserialized.get("key2")).isEqualTo("value2");
    }

    @Test
    public void testMapOfThreeEntries() {
        Map<String, Integer> original = Map.of("one", 1, "two", 2, "three", 3);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.get("one")).isEqualTo(1);
        assertThat(deserialized.get("two")).isEqualTo(2);
        assertThat(deserialized.get("three")).isEqualTo(3);
    }

    @Test
    public void testMapOfFourEntries() {
        Map<String, Integer> original = Map.of("one", 1, "two", 2, "three", 3, "four", 4);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(4);
        assertThat(deserialized).containsAllEntriesOf(original);
    }

    @Test
    public void testMapOfFiveEntries() {
        Map<String, Integer> original = Map.of("one", 1, "two", 2, "three", 3, "four", 4, "five", 5);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(5);
        assertThat(deserialized).containsAllEntriesOf(original);
    }

    @Test
    public void testMapOfMultipleEntries() {
        Map<String, Integer> original = Map.of(
            "one", 1,
            "two", 2,
            "three", 3,
            "four", 4,
            "five", 5,
            "six", 6,
            "seven", 7,
            "eight", 8
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(8);
        assertThat(deserialized).containsAllEntriesOf(original);
    }

    @Test
    public void testMapOfWithIntegerKeys() {
        Map<Integer, String> original = Map.of(
            1, "one",
            2, "two",
            3, "three",
            4, "four",
            5, "five"
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<Integer, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(5);
        assertThat(deserialized).containsAllEntriesOf(original);
    }

    @Test
    public void testMapOfIsImmutable() {
        Map<String, String> original = Map.of("key1", "value1", "key2", "value2");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.put("key3", "value3"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testMapOfNested() {
        Map<String, Map<String, Integer>> original = Map.of(
            "map1", Map.of("a", 1, "b", 2),
            "map2", Map.of("c", 3, "d", 4)
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Integer>> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get("map1")).containsEntry("a", 1).containsEntry("b", 2);
        assertThat(deserialized.get("map2")).containsEntry("c", 3).containsEntry("d", 4);
    }

    @Test
    public void testMapOfEntriesLarge() {
        Map<Integer, String> original = Map.ofEntries(
            Map.entry(1, "one"),
            Map.entry(2, "two"),
            Map.entry(3, "three"),
            Map.entry(4, "four"),
            Map.entry(5, "five"),
            Map.entry(6, "six"),
            Map.entry(7, "seven"),
            Map.entry(8, "eight"),
            Map.entry(9, "nine"),
            Map.entry(10, "ten"),
            Map.entry(11, "eleven"),
            Map.entry(12, "twelve")
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<Integer, String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(12);
        assertThat(deserialized).containsAllEntriesOf(original);
    }

    // ========== Mixed Types Tests ==========

    @Test
    public void testMapOfWithMixedValueTypes() {
        Map<String, Number> original = Map.of(
            "int", 1,
            "long", 2L,
            "float", 3.0f,
            "double", 4.0
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Number> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(4);
        assertThat(deserialized.get("int")).isEqualTo(1);
        assertThat(deserialized.get("long")).isEqualTo(2L);
        assertThat(deserialized.get("float")).isEqualTo(3.0f);
        assertThat(deserialized.get("double")).isEqualTo(4.0);
    }

    @Test
    public void testMapOfStringToList() {
        Map<String, java.util.List<Integer>> original = Map.of(
            "list1", java.util.List.of(1, 2, 3),
            "list2", java.util.List.of(4, 5, 6)
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, java.util.List<Integer>> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(2);
        assertThat(deserialized.get("list1")).containsExactly(1, 2, 3);
        assertThat(deserialized.get("list2")).containsExactly(4, 5, 6);
    }

    // ========== Null Handling ==========

    @Test
    public void testNullImmutableMap() {
        byte[] bytes = serializer.serialize(null);
        Map<?, ?> deserialized = serializer.deserialize(bytes, Map.class);
        assertThat(deserialized).isNull();
    }
}
