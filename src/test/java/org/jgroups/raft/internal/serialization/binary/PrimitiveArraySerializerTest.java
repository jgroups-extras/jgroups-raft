package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.Serializer;

import java.util.Random;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for primitive array serialization with {@link BinarySerializer}.
 *
 * <p>
 * This test class validates serialization and deserialization for all primitive array types supported by the binary serializer,
 * including edge cases such as empty arrays, large arrays, and arrays with extreme values.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class PrimitiveArraySerializerTest {

    private Serializer serializer;

    @BeforeMethod
    public void setUp() {
        SerializationRegistry registry = SerializationRegistry.create();
        serializer = Serializer.create(registry);
    }

    // ========== Byte Array Tests ==========

    @Test
    public void testByteArraySerializationRoundTrip() {
        byte[] original = {1, 2, 3, 4, 5};
        byte[] bytes = serializer.serialize(original);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testEmptyByteArray() {
        byte[] original = {};
        byte[] bytes = serializer.serialize(original);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testByteArrayMinMaxValues() {
        byte[] original = {Byte.MIN_VALUE, Byte.MAX_VALUE, 0};
        byte[] bytes = serializer.serialize(original);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).containsExactly(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0);
    }

    @Test
    public void testLargeByteArray() {
        byte[] original = new byte[10000];
        new Random().nextBytes(original);
        byte[] bytes = serializer.serialize(original);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).isEqualTo(original);
    }

    // ========== Short Array Tests ==========

    @Test
    public void testShortArraySerializationRoundTrip() {
        short[] original = {1, 2, 3, 4, 5};
        byte[] bytes = serializer.serialize(original);
        short[] deserialized = serializer.deserialize(bytes, short[].class);
        assertThat(deserialized).containsExactly((short) 1, (short) 2, (short) 3, (short) 4, (short) 5);
    }

    @Test
    public void testEmptyShortArray() {
        short[] original = {};
        byte[] bytes = serializer.serialize(original);
        short[] deserialized = serializer.deserialize(bytes, short[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testShortArrayMinMaxValues() {
        short[] original = {Short.MIN_VALUE, Short.MAX_VALUE, 0, 1000, -1000};
        byte[] bytes = serializer.serialize(original);
        short[] deserialized = serializer.deserialize(bytes, short[].class);
        assertThat(deserialized).containsExactly(Short.MIN_VALUE, Short.MAX_VALUE, (short) 0, (short) 1000, (short) -1000);
    }

    // ========== Int Array Tests ==========

    @Test
    public void testIntArraySerializationRoundTrip() {
        int[] original = {1, 2, 3, 4, 5};
        byte[] bytes = serializer.serialize(original);
        int[] deserialized = serializer.deserialize(bytes, int[].class);
        assertThat(deserialized).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testEmptyIntArray() {
        int[] original = {};
        byte[] bytes = serializer.serialize(original);
        int[] deserialized = serializer.deserialize(bytes, int[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testIntArrayMinMaxValues() {
        int[] original = {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 123456, -123456};
        byte[] bytes = serializer.serialize(original);
        int[] deserialized = serializer.deserialize(bytes, int[].class);
        assertThat(deserialized).containsExactly(Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 123456, -123456);
    }

    @Test
    public void testLargeIntArray() {
        int[] original = new int[1000];
        for (int i = 0; i < original.length; i++) {
            original[i] = i;
        }
        byte[] bytes = serializer.serialize(original);
        int[] deserialized = serializer.deserialize(bytes, int[].class);
        assertThat(deserialized).isEqualTo(original);
    }

    // ========== Long Array Tests ==========

    @Test
    public void testLongArraySerializationRoundTrip() {
        long[] original = {1L, 2L, 3L, 4L, 5L};
        byte[] bytes = serializer.serialize(original);
        long[] deserialized = serializer.deserialize(bytes, long[].class);
        assertThat(deserialized).containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    @Test
    public void testEmptyLongArray() {
        long[] original = {};
        byte[] bytes = serializer.serialize(original);
        long[] deserialized = serializer.deserialize(bytes, long[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testLongArrayMinMaxValues() {
        long[] original = {Long.MIN_VALUE, Long.MAX_VALUE, 0L, 123456789L, -123456789L};
        byte[] bytes = serializer.serialize(original);
        long[] deserialized = serializer.deserialize(bytes, long[].class);
        assertThat(deserialized).containsExactly(Long.MIN_VALUE, Long.MAX_VALUE, 0L, 123456789L, -123456789L);
    }

    // ========== Float Array Tests ==========

    @Test
    public void testFloatArraySerializationRoundTrip() {
        float[] original = {1.0f, 2.5f, 3.14159f, -4.0f, 0.0f};
        byte[] bytes = serializer.serialize(original);
        float[] deserialized = serializer.deserialize(bytes, float[].class);
        assertThat(deserialized).containsExactly(1.0f, 2.5f, 3.14159f, -4.0f, 0.0f);
    }

    @Test
    public void testEmptyFloatArray() {
        float[] original = {};
        byte[] bytes = serializer.serialize(original);
        float[] deserialized = serializer.deserialize(bytes, float[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testFloatArraySpecialValues() {
        float[] original = {Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f};
        byte[] bytes = serializer.serialize(original);
        float[] deserialized = serializer.deserialize(bytes, float[].class);
        assertThat(deserialized).containsExactly(Float.MIN_VALUE, Float.MAX_VALUE, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f);
    }

    // ========== Double Array Tests ==========

    @Test
    public void testDoubleArraySerializationRoundTrip() {
        double[] original = {1.0, 2.5, 3.141592653589793, -4.0, 0.0};
        byte[] bytes = serializer.serialize(original);
        double[] deserialized = serializer.deserialize(bytes, double[].class);
        assertThat(deserialized).containsExactly(1.0, 2.5, 3.141592653589793, -4.0, 0.0);
    }

    @Test
    public void testEmptyDoubleArray() {
        double[] original = {};
        byte[] bytes = serializer.serialize(original);
        double[] deserialized = serializer.deserialize(bytes, double[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testDoubleArraySpecialValues() {
        double[] original = {Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0};
        byte[] bytes = serializer.serialize(original);
        double[] deserialized = serializer.deserialize(bytes, double[].class);
        assertThat(deserialized).containsExactly(Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0);
    }

    // ========== Boolean Array Tests ==========

    @Test
    public void testBooleanArraySerializationRoundTrip() {
        boolean[] original = {true, false, true, true, false};
        byte[] bytes = serializer.serialize(original);
        boolean[] deserialized = serializer.deserialize(bytes, boolean[].class);
        assertThat(deserialized).containsExactly(true, false, true, true, false);
    }

    @Test
    public void testEmptyBooleanArray() {
        boolean[] original = {};
        byte[] bytes = serializer.serialize(original);
        boolean[] deserialized = serializer.deserialize(bytes, boolean[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testBooleanArrayAllTrue() {
        boolean[] original = new boolean[100];
        for (int i = 0; i < original.length; i++) {
            original[i] = true;
        }
        byte[] bytes = serializer.serialize(original);
        boolean[] deserialized = serializer.deserialize(bytes, boolean[].class);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    public void testBooleanArrayAllFalse() {
        boolean[] original = new boolean[100]; // Default is false
        byte[] bytes = serializer.serialize(original);
        boolean[] deserialized = serializer.deserialize(bytes, boolean[].class);
        assertThat(deserialized).isEqualTo(original);
    }

    // ========== Char Array Tests ==========

    @Test
    public void testCharArraySerializationRoundTrip() {
        char[] original = {'a', 'b', 'c', 'd', 'e'};
        byte[] bytes = serializer.serialize(original);
        char[] deserialized = serializer.deserialize(bytes, char[].class);
        assertThat(deserialized).containsExactly('a', 'b', 'c', 'd', 'e');
    }

    @Test
    public void testEmptyCharArray() {
        char[] original = {};
        byte[] bytes = serializer.serialize(original);
        char[] deserialized = serializer.deserialize(bytes, char[].class);
        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testCharArrayWithSpecialCharacters() {
        char[] original = {'A', 'z', '0', '9', '\n', '\t', ' ', '日', '本'};
        byte[] bytes = serializer.serialize(original);
        char[] deserialized = serializer.deserialize(bytes, char[].class);
        assertThat(deserialized).containsExactly('A', 'z', '0', '9', '\n', '\t', ' ', '日', '本');
    }

    @Test
    public void testCharArrayMinMaxValues() {
        char[] original = {Character.MIN_VALUE, Character.MAX_VALUE, '\0'};
        byte[] bytes = serializer.serialize(original);
        char[] deserialized = serializer.deserialize(bytes, char[].class);
        assertThat(deserialized).containsExactly(Character.MIN_VALUE, Character.MAX_VALUE, '\0');
    }

    // ========== Null Handling ==========

    @Test
    public void testNullByteArray() {
        byte[] bytes = serializer.serialize(null);
        byte[] deserialized = serializer.deserialize(bytes, byte[].class);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testNullIntArray() {
        byte[] bytes = serializer.serialize(null);
        int[] deserialized = serializer.deserialize(bytes, int[].class);
        assertThat(deserialized).isNull();
    }

    // ========== Helper Methods ==========

    /**
     * Helper method to test serialization round-trip for any array type.
     */
    private <T> void assertSerializationRoundTrip(T original, Class<T> type) {
        byte[] bytes = serializer.serialize(original);
        T deserialized = serializer.deserialize(bytes, type);
        assertThat(deserialized).isEqualTo(original);
    }
}
