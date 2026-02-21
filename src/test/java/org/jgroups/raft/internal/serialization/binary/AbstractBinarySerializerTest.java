package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.util.io.CustomByteBuffer;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Abstract base class for binary serialization tests.
 *
 * <p>
 * Provides common infrastructure for testing serializers:
 * <ul>
 *   <li>Registry setup and teardown</li>
 *   <li>Serialize/deserialize helper methods</li>
 *   <li>Round-trip verification</li>
 *   <li>Version handling</li>
 * </ul>
 * </p>
 *
 * <p>
 * Subclasses should:
 * <ul>
 *   <li>Override {@link #registerSerializers(BinarySerializationRegistry)} to register their serializers</li>
 *   <li>Provide test methods that use {@link #assertSerializationRoundTrip(Object, Class)}</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public abstract class AbstractBinarySerializerTest {

    private BinarySerializationRegistry registry;

    @BeforeMethod
    protected void setUp() {
        registry = new BinarySerializationRegistry();
        registerSerializers();
    }


    /**
     * Return the serializers utilized for testing.
     *
     * <p>
     * Subclasses should override this to register their specific serializers.
     * </p>
     * @return The test serializer.
     */
    protected abstract SingleBinarySerializer<?>[] serializers();

    private void registerSerializers() {
        for (SingleBinarySerializer<?> serializer : serializers()) {
            registry.registerSerializer(serializer);
        }
    }

    /**
     * Serializes an object using the binary format.
     *
     * @param object The object to serialize
     * @return The serialized bytes
     */
    protected byte[] serialize(Object object) {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(512);
        DefaultSerializationContext ctx = new DefaultSerializationContext(registry, buffer);

        ctx.writeObject(object);

        return buffer.toByteArray();
    }

    /**
     * Deserializes bytes back into an object.
     *
     * @param bytes The serialized bytes
     * @param expectedType The expected type of the deserialized object
     * @param <T> The type parameter
     * @return The deserialized object
     */
    protected <T> T deserialize(byte[] bytes, Class<T> expectedType) {
        CustomByteBuffer buffer = CustomByteBuffer.wrap(bytes);
        DefaultSerializationContext ctx = new DefaultSerializationContext(registry, buffer);

        T result = ctx.readObject();

        // Verify type
        if (result != null) {
            assertThat(result).isInstanceOf(expectedType);
        }

        return result;
    }

    /**
     * Tests full serialization round-trip for an object.
     *
     * <p>
     * Serializes the object, deserializes it, and verifies equality.
     * </p>
     *
     * @param original The object to test
     * @param expectedType The expected type
     * @param <T> The type parameter
     */
    protected <T> void assertSerializationRoundTrip(T original, Class<T> expectedType) {
        // Serialize
        byte[] bytes = serialize(original);

        assertThat(bytes).isNotNull();
        assertThat(bytes.length).isGreaterThan(0);

        // Deserialize
        T deserialized = deserialize(bytes, expectedType);

        // Verify
        assertThat(deserialized).isEqualTo(original);
    }

    /**
     * Tests that null objects are properly handled.
     */
    @Test
    public void testNullSerialization() {
        byte[] bytes = serialize(null);

        assertThat(bytes).isNotNull();
        assertThat(bytes.length).isEqualTo(4); // Just the NULL_OBJECT marker (int)

        Object deserialized = deserialize(bytes, Object.class);
        assertThat(deserialized).isNull();
    }

    /**
     * Verifies the wire format for a serialized object.
     *
     * @param bytes The serialized bytes
     * @param expectedTypeId The expected type ID in the wire format
     * @param expectedVersion The expected version in the wire format
     */
    protected void assertWireFormat(byte[] bytes, int expectedTypeId, byte expectedVersion, int expectedLength) {
        assertThat(bytes.length).isGreaterThanOrEqualTo(9); // type-id (4) + version (1) + size (4) + data

        CustomByteBuffer buffer = CustomByteBuffer.wrap(bytes);

        int typeId = buffer.readInt();
        assertThat(typeId).isEqualTo(expectedTypeId);

        byte version = buffer.readByte();
        assertThat(version).isEqualTo(expectedVersion);

        int length = buffer.readInt();
        assertThat(length).isEqualTo(expectedLength);
    }

    /**
     * Helper to verify serializer metadata.
     *
     * @param serializer The serializer to check
     * @param expectedClass The expected class
     * @param expectedTypeId The expected type ID
     * @param expectedVersion The expected version
     * @param <T> The type parameter
     */
    protected <T> void assertSerializerMetadata(
            SingleBinarySerializer<T> serializer,
            Class<T> expectedClass,
            int expectedTypeId,
            byte expectedVersion) {

        assertThat(serializer.javaClass()).isEqualTo(expectedClass);
        assertThat(serializer.type()).isEqualTo(expectedTypeId);
        assertThat(serializer.version()).isEqualTo(expectedVersion);
    }
}
