package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link BinarySerializationRegistry}.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class BinarySerializationRegistryTest {

    private BinarySerializationRegistry registry;

    @BeforeMethod
    public void setUp() {
        registry = new BinarySerializationRegistry();
    }

    @Test
    public void testRegisterSerializer() {
        TestSerializer serializer = new TestSerializer(TestClass.class, 100);

        registry.registerSerializer(serializer);

        // Should be able to find by class
        SingleBinarySerializer<?> found = registry.findSerializer(TestClass.class);
        assertThat(found).isSameAs(serializer);

        // Should be able to find by type ID
        SingleBinarySerializer<?> foundById = registry.findDeserializer(100);
        assertThat(foundById).isSameAs(serializer);
    }

    @Test
    public void testRegisterMultipleSerializers() {
        TestSerializer serializer1 = new TestSerializer(TestClass.class, 100);
        TestSerializer serializer2 = new TestSerializer(AnotherTestClass.class, 101);

        registry.registerSerializer(serializer1);
        registry.registerSerializer(serializer2);

        assertThat(registry.findSerializer(TestClass.class)).isSameAs(serializer1);
        assertThat(registry.findSerializer(AnotherTestClass.class)).isSameAs(serializer2);
        assertThat(registry.findDeserializer(100)).isSameAs(serializer1);
        assertThat(registry.findDeserializer(101)).isSameAs(serializer2);
    }

    @Test
    public void testDuplicateClassRegistrationThrowsException() {
        TestSerializer serializer1 = new TestSerializer(TestClass.class, 100);
        TestSerializer serializer2 = new TestSerializer(TestClass.class, 200);

        registry.registerSerializer(serializer1);

        assertThatThrownBy(() -> registry.registerSerializer(serializer2))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Serializer for class")
            .hasMessageContaining("was already registered")
            .hasMessageContaining(TestClass.class.getName());
    }

    @Test
    public void testDuplicateTypeIdRegistrationThrowsException() {
        TestSerializer serializer1 = new TestSerializer(TestClass.class, 100);
        TestSerializer serializer2 = new TestSerializer(AnotherTestClass.class, 100);

        registry.registerSerializer(serializer1);

        assertThatThrownBy(() -> registry.registerSerializer(serializer2))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Serializer with type id")
            .hasMessageContaining("was already registered")
            .hasMessageContaining("100");
    }

    @Test
    public void testFindSerializerNotFoundThrowsException() {
        assertThatThrownBy(() -> registry.findSerializer(TestClass.class))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Serializer not found for type")
            .hasMessageContaining(TestClass.class.getName())
            .hasMessageContaining("JGroupsRaftCustomMarshaller");
    }

    @Test
    public void testFindDeserializerNotFoundThrowsException() {
        assertThatThrownBy(() -> registry.findDeserializer(999))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Serializer not found for id")
            .hasMessageContaining("999")
            .hasMessageContaining("JGroupsRaftCustomMarshaller");
    }

    @Test
    public void testRegisterNullSerializerThrowsException() {
        assertThatThrownBy(() -> registry.registerSerializer(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("serializer may not be null");
    }

    @Test
    public void testRegisterSerializerWithNullClassThrowsException() {
        TestSerializer serializerWithNullClass = new TestSerializer(null, 100);

        assertThatThrownBy(() -> registry.registerSerializer(serializerWithNullClass))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("serializer class may not be null");
    }

    @Test
    public void testFindAfterMultipleRegistrations() {
        // Register several serializers
        for (int i = 0; i < 10; i++) {
            TestSerializer serializer = new TestSerializer(createTestClass(i), 100 + i);
            registry.registerSerializer(serializer);
        }

        // Find each one
        for (int i = 0; i < 10; i++) {
            Class<?> clazz = createTestClass(i);
            SingleBinarySerializer<?> found = registry.findSerializer(clazz);
            assertThat(found).isNotNull();
            assertThat(found.type()).isEqualTo(100 + i);
        }
    }

    // Helper to create unique test classes
    private Class<?> createTestClass(int index) {
        return switch (index) {
            case 0 -> TestClass.class;
            case 1 -> AnotherTestClass.class;
            case 2 -> YetAnotherTestClass.class;
            case 3 -> TestClass4.class;
            case 4 -> TestClass5.class;
            case 5 -> TestClass6.class;
            case 6 -> TestClass7.class;
            case 7 -> TestClass8.class;
            case 8 -> TestClass9.class;
            case 9 -> TestClass10.class;
            default -> throw new IllegalArgumentException("Index out of range");
        };
    }

    // Test classes
    private static class TestClass {}
    private static class AnotherTestClass {}
    private static class YetAnotherTestClass {}
    private static class TestClass4 {}
    private static class TestClass5 {}
    private static class TestClass6 {}
    private static class TestClass7 {}
    private static class TestClass8 {}
    private static class TestClass9 {}
    private static class TestClass10 {}

    /**
     * Simple test serializer implementation.
     */
    private static class TestSerializer implements SingleBinarySerializer<Object> {
        private final Class<?> javaClass;
        private final int typeId;

        TestSerializer(Class<?> javaClass, int typeId) {
            this.javaClass = javaClass;
            this.typeId = typeId;
        }

        @Override
        public void write(SerializationContextWrite ctx, Object target) {
            // No-op for testing
        }

        @Override
        public Object read(SerializationContextRead ctx, byte version) {
            // No-op for testing
            return null;
        }

        @Override
        public Class<Object> javaClass() {
            @SuppressWarnings("unchecked")
            Class<Object> clazz = (Class<Object>) javaClass;
            return clazz;
        }

        @Override
        public int type() {
            return typeId;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
