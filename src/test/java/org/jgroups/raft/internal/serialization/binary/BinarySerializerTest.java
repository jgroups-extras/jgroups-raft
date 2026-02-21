package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.Serializer;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link BinarySerializer} with all supported built-in Java types.
 *
 * <p>
 * This test class contains test methods for all the types that should be supported
 * by the binary serializer. Tests are initially disabled and will be enabled as
 * serializers for each type are implemented.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class BinarySerializerTest {

    private Serializer serializer;

    @BeforeMethod
    public void setUp() {
        SerializationRegistry registry = SerializationRegistry.create();
        // User serializers would be registered here
        // For now, we're testing built-in types only

        serializer = Serializer.create(registry);
    }

    // ========== Primitive Wrapper Types ==========

    public void testByteSerializationRoundTrip() {
        byte original = (byte) 42;
        byte[] bytes = serializer.serialize(original);
        byte deserialized = serializer.deserialize(bytes, byte.class);
        assertThat(deserialized).isEqualTo(original);
    }

    public void testByteMinMaxValues() {
        assertSerializationRoundTrip(Byte.MIN_VALUE, Byte.class);
        assertSerializationRoundTrip(Byte.MAX_VALUE, Byte.class);
        assertSerializationRoundTrip((byte) 0, Byte.class);
    }

    public void testShortSerializationRoundTrip() {
        short original = (short) 1000;
        byte[] bytes = serializer.serialize(original);
        short deserialized = serializer.deserialize(bytes, short.class);
        assertThat(deserialized).isEqualTo(original);
    }

    public void testShortMinMaxValues() {
        assertSerializationRoundTrip(Short.MIN_VALUE, Short.class);
        assertSerializationRoundTrip(Short.MAX_VALUE, Short.class);
        assertSerializationRoundTrip((short) 0, Short.class);
    }

    public void testIntegerSerializationRoundTrip() {
        int original = 123456;
        byte[] bytes = serializer.serialize(original);
        int deserialized = serializer.deserialize(bytes, int.class);
        assertThat(deserialized).isEqualTo(original);
    }

    public void testIntegerMinMaxValues() {
        assertSerializationRoundTrip(Integer.MIN_VALUE, Integer.class);
        assertSerializationRoundTrip(Integer.MAX_VALUE, Integer.class);
        assertSerializationRoundTrip(0, int.class);
    }

    public void testLongSerializationRoundTrip() {
        long original = 123456789L;
        byte[] bytes = serializer.serialize(original);
        long deserialized = serializer.deserialize(bytes, long.class);
        assertThat(deserialized).isEqualTo(original);
    }

    public void testLongMinMaxValues() {
        assertSerializationRoundTrip(Long.MIN_VALUE, Long.class);
        assertSerializationRoundTrip(Long.MAX_VALUE, Long.class);
        assertSerializationRoundTrip(0L, Long.class);
    }

    public void testFloatSerializationRoundTrip() {
        float original = 3.14159f;
        byte[] bytes = serializer.serialize(original);
        float deserialized = serializer.deserialize(bytes, float.class);
        assertThat(deserialized).isEqualTo(original);
    }

    public void testFloatSpecialValues() {
        assertSerializationRoundTrip(Float.MIN_VALUE, Float.class);
        assertSerializationRoundTrip(Float.MAX_VALUE, Float.class);
        assertSerializationRoundTrip(0.0f, Float.class);
        assertSerializationRoundTrip(Float.NaN, Float.class);
        assertSerializationRoundTrip(Float.POSITIVE_INFINITY, Float.class);
        assertSerializationRoundTrip(Float.NEGATIVE_INFINITY, Float.class);
    }

    public void testDoubleSerializationRoundTrip() {
        double original = 3.141592653589793;
        byte[] bytes = serializer.serialize(original);
        double deserialized = serializer.deserialize(bytes, double.class);
        assertThat(deserialized).isEqualTo(original);
    }

    public void testDoubleSpecialValues() {
        assertSerializationRoundTrip(Double.MIN_VALUE, Double.class);
        assertSerializationRoundTrip(Double.MAX_VALUE, Double.class);
        assertSerializationRoundTrip(0.0, Double.class);
        assertSerializationRoundTrip(Double.NaN, Double.class);
        assertSerializationRoundTrip(Double.POSITIVE_INFINITY, Double.class);
        assertSerializationRoundTrip(Double.NEGATIVE_INFINITY, Double.class);
    }

    @Test(enabled = false, description = "Enable when BooleanSerializer is implemented")
    public void testBooleanSerializationRoundTrip() {
        assertSerializationRoundTrip(Boolean.TRUE, Boolean.class);
        assertSerializationRoundTrip(Boolean.FALSE, Boolean.class);
    }

    @Test(enabled = false, description = "Enable when CharacterSerializer is implemented")
    public void testCharacterSerializationRoundTrip() {
        assertSerializationRoundTrip('A', Character.class);
        assertSerializationRoundTrip('z', Character.class);
        assertSerializationRoundTrip('0', Character.class);
        assertSerializationRoundTrip('日', Character.class); // Unicode
        assertSerializationRoundTrip('\n', Character.class);
        assertSerializationRoundTrip('\0', Character.class);
    }

    // ========== String ==========
    public void testStringSerializationRoundTrip() {
        String original = "Hello, World!";
        byte[] bytes = serializer.serialize(original);
        String deserialized = serializer.deserialize(bytes, String.class);
        assertThat(deserialized).isEqualTo(original);
    }

    public void testEmptyString() {
        assertSerializationRoundTrip("", String.class);
    }

    public void testStringWithSpecialCharacters() {
        assertSerializationRoundTrip("Special: \n\r\t\"'\\", String.class);
        assertSerializationRoundTrip("Unicode: 日本語 🎉 ñ é", String.class);
    }

    public void testLongString() {
        String longString = "a".repeat(10000);
        assertSerializationRoundTrip(longString, String.class);
    }

    // ========== Byte Array ==========

    public void testByteArraySerializationRoundTrip() {
        byte[] original = {1, 2, 3, 4, 5};
        byte[] bytes = serializer.serialize(original);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).containsExactly(1, 2, 3, 4, 5);
    }

    public void testEmptyByteArray() {
        byte[] original = {};
        byte[] bytes = serializer.serialize(original);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).isEmpty();
    }

    public void testLargeByteArray() {
        byte[] original = new byte[10000];
        new Random().nextBytes(original);
        byte[] bytes = serializer.serialize(original);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).isEqualTo(original);
    }

    // ========== Date and Time Types ==========

    @Test(enabled = false, description = "Enable when InstantSerializer is implemented")
    public void testInstantSerializationRoundTrip() {
        Instant original = Instant.now();
        byte[] bytes = serializer.serialize(original);
        Instant deserialized = serializer.deserialize(bytes, Instant.class);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test(enabled = false, description = "Enable when InstantSerializer is implemented")
    public void testInstantEpoch() {
        assertSerializationRoundTrip(Instant.EPOCH, Instant.class);
    }

    @Test(enabled = false, description = "Enable when InstantSerializer is implemented")
    public void testInstantMinMax() {
        assertSerializationRoundTrip(Instant.MIN, Instant.class);
        assertSerializationRoundTrip(Instant.MAX, Instant.class);
    }

    @Test(enabled = false, description = "Enable when DateSerializer is implemented")
    public void testDateSerializationRoundTrip() {
        Date original = new Date();
        byte[] bytes = serializer.serialize(original);
        Date deserialized = serializer.deserialize(bytes, Date.class);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test(enabled = false, description = "Enable when DateSerializer is implemented")
    public void testDateEpoch() {
        assertSerializationRoundTrip(new Date(0), Date.class);
    }

    // ========== Collections ==========

    @Test(enabled = false, description = "Enable when ListSerializer is implemented")
    public void testListSerializationRoundTrip() {
        List<String> original = Arrays.asList("one", "two", "three");
        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, List.class);
        assertThat(deserialized).containsExactly("one", "two", "three");
    }

    @Test(enabled = false, description = "Enable when ListSerializer is implemented")
    public void testEmptyList() {
        List<String> original = Collections.emptyList();
        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, List.class);
        assertThat(deserialized).isEmpty();
    }

    @Test(enabled = false, description = "Enable when ListSerializer is implemented")
    public void testListWithDifferentTypes() {
        // List of integers
        List<Integer> original = Arrays.asList(1, 2, 3, 4, 5);
        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<Integer> deserialized = serializer.deserialize(bytes, List.class);
        assertThat(deserialized).containsExactly(1, 2, 3, 4, 5);
    }

    @Test(enabled = false, description = "Enable when SetSerializer is implemented")
    public void testSetSerializationRoundTrip() {
        Set<String> original = new HashSet<>(Arrays.asList("one", "two", "three"));
        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, Set.class);
        assertThat(deserialized).containsExactlyInAnyOrder("one", "two", "three");
    }

    @Test(enabled = false, description = "Enable when SetSerializer is implemented")
    public void testEmptySet() {
        Set<String> original = Collections.emptySet();
        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, Set.class);
        assertThat(deserialized).isEmpty();
    }

    @Test(enabled = false, description = "Enable when MapSerializer is implemented")
    public void testMapSerializationRoundTrip() {
        Map<String, Integer> original = new HashMap<>();
        original.put("one", 1);
        original.put("two", 2);
        original.put("three", 3);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, Integer> deserialized = serializer.deserialize(bytes, Map.class);

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.get("one")).isEqualTo(1);
        assertThat(deserialized.get("two")).isEqualTo(2);
        assertThat(deserialized.get("three")).isEqualTo(3);
    }

    @Test(enabled = false, description = "Enable when MapSerializer is implemented")
    public void testEmptyMap() {
        Map<String, String> original = Collections.emptyMap();
        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Map<String, String> deserialized = serializer.deserialize(bytes, Map.class);
        assertThat(deserialized).isEmpty();
    }

    // ========== Null Handling ==========

    @Test
    public void testNullSerialization() {
        byte[] bytes = serializer.serialize(null);
        Object deserialized = serializer.deserialize(bytes, Object.class);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testNullBuffer() {
        Object deserialized = serializer.deserialize(null, Object.class);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testEmptyBuffer() {
        Object deserialized = serializer.deserialize(new byte[0], Object.class);
        assertThat(deserialized).isNull();
    }

    // ========== Helper Methods ==========

    /**
     * Helper method to test serialization round-trip for any object.
     */
    private <T> void assertSerializationRoundTrip(T original, Class<T> type) {
        byte[] bytes = serializer.serialize(original);
        T deserialized = serializer.deserialize(bytes, type);
        assertThat(deserialized).isEqualTo(original);
    }
}
