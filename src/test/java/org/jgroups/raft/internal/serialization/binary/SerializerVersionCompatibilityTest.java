package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;
import org.jgroups.raft.util.io.CustomByteBuffer;

import java.util.Objects;

import org.testng.annotations.Test;

/**
 * Tests for forward and backward compatibility of serializers across versions.
 *
 * <p>
 * This test verifies that:
 * <ul>
 *   <li>Older serializer versions can read data written by newer versions (forward compatibility)</li>
 *   <li>Newer serializer versions can read data written by older versions (backward compatibility)</li>
 *   <li>Unknown trailing bytes are properly skipped when reading older versions</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SerializerVersionCompatibilityTest {

    private static final int TYPE_PERSON = 2000;

    /**
     * Tests backward compatibility: Version 2 serializer reading Version 1 data.
     */
    @Test
    public void testBackwardCompatibility() {
        // Write with version 1 serializer
        BinarySerializationRegistry registryV1 = new BinarySerializationRegistry();
        registryV1.registerSerializer(new PersonSerializerV1());

        Person personV1 = new Person("Alice", 30, null);
        byte[] bytesFromV1 = serialize(registryV1, personV1);

        // Read with version 2 serializer
        BinarySerializationRegistry registryV2 = new BinarySerializationRegistry();
        registryV2.registerSerializer(new PersonSerializerV2());

        Person deserializedWithV2 = deserialize(registryV2, bytesFromV1, Person.class);

        // Should successfully read, with default value for missing email field
        assertThat(deserializedWithV2.name).isEqualTo("Alice");
        assertThat(deserializedWithV2.age).isEqualTo(30);
        assertThat(deserializedWithV2.email).isEqualTo("unknown@example.com"); // Default value
    }

    /**
     * Tests forward compatibility: Version 1 serializer reading Version 2 data.
     *
     * <p>
     * This is the critical test - ensures that old code can read new data by skipping
     * unknown trailing bytes.
     * </p>
     */
    @Test
    public void testForwardCompatibility() {
        // Write with version 2 serializer (includes email field)
        BinarySerializationRegistry registryV2 = new BinarySerializationRegistry();
        registryV2.registerSerializer(new PersonSerializerV2());

        Person personV2 = new Person("Bob", 25, "bob@example.com");
        byte[] bytesFromV2 = serialize(registryV2, personV2);

        // Read with version 1 serializer (doesn't know about email field)
        BinarySerializationRegistry registryV1 = new BinarySerializationRegistry();
        registryV1.registerSerializer(new PersonSerializerV1());

        Person deserializedWithV1 = deserialize(registryV1, bytesFromV2, Person.class);

        // Should successfully read, ignoring the email field
        assertThat(deserializedWithV1.name).isEqualTo("Bob");
        assertThat(deserializedWithV1.age).isEqualTo(25);
        assertThat(deserializedWithV1.email).isNull(); // V1 doesn't set this
    }

    /**
     * Tests that multiple objects in sequence maintain proper boundaries.
     *
     * <p>
     * Ensures that skipping unknown bytes doesn't corrupt subsequent object reads.
     * </p>
     */
    @Test
    public void testMultipleObjectsWithVersionMismatch() {
        // Write multiple objects with V2
        BinarySerializationRegistry registryV2 = new BinarySerializationRegistry();
        registryV2.registerSerializer(new PersonSerializerV2());

        CustomByteBuffer buffer = CustomByteBuffer.allocate(512);
        DefaultSerializationContext ctxWrite = new DefaultSerializationContext(registryV2, buffer);

        // Write 3 Person objects
        ctxWrite.writeObject(new Person("Alice", 30, "alice@example.com"));
        ctxWrite.writeObject(new Person("Bob", 25, "bob@example.com"));
        ctxWrite.writeObject(new Person("Charlie", 35, "charlie@example.com"));

        byte[] bytes = buffer.toByteArray();

        // Read with V1 serializer
        BinarySerializationRegistry registryV1 = new BinarySerializationRegistry();
        registryV1.registerSerializer(new PersonSerializerV1());

        CustomByteBuffer readBuffer = CustomByteBuffer.wrap(bytes);
        DefaultSerializationContext ctxRead = new DefaultSerializationContext(registryV1, readBuffer);

        // All three should be readable despite version mismatch
        Person p1 = ctxRead.readObject();
        assertThat(p1.name).isEqualTo("Alice");
        assertThat(p1.age).isEqualTo(30);

        Person p2 = ctxRead.readObject();
        assertThat(p2.name).isEqualTo("Bob");
        assertThat(p2.age).isEqualTo(25);

        Person p3 = ctxRead.readObject();
        assertThat(p3.name).isEqualTo("Charlie");
        assertThat(p3.age).isEqualTo(35);

        // Buffer should be fully consumed
        assertThat(readBuffer.readableBytes()).isEqualTo(0);
    }

    /**
     * Tests that V3 can read V1 data (skipping two versions).
     */
    @Test
    public void testMultipleVersionJump() {
        // Write with V1
        BinarySerializationRegistry registryV1 = new BinarySerializationRegistry();
        registryV1.registerSerializer(new PersonSerializerV1());

        Person personV1 = new Person("Dave", 40, null);
        byte[] bytesFromV1 = serialize(registryV1, personV1);

        // Read with V3 (which added phone field after email)
        BinarySerializationRegistry registryV3 = new BinarySerializationRegistry();
        registryV3.registerSerializer(new PersonSerializerV3());

        Person deserializedWithV3 = deserialize(registryV3, bytesFromV1, Person.class);

        assertThat(deserializedWithV3.name).isEqualTo("Dave");
        assertThat(deserializedWithV3.age).isEqualTo(40);
        assertThat(deserializedWithV3.email).isEqualTo("unknown@example.com"); // Default
        assertThat(deserializedWithV3.phone).isEqualTo("000-000-0000"); // Default
    }

    /**
     * Tests that V1 can read V3 data (skipping multiple new fields).
     */
    @Test
    public void testForwardCompatibilityMultipleFields() {
        // Write with V3 (has email and phone)
        BinarySerializationRegistry registryV3 = new BinarySerializationRegistry();
        registryV3.registerSerializer(new PersonSerializerV3());

        Person personV3 = new Person("Eve", 28, "eve@example.com", "555-1234");
        byte[] bytesFromV3 = serialize(registryV3, personV3);

        // Read with V1 (knows only name and age)
        BinarySerializationRegistry registryV1 = new BinarySerializationRegistry();
        registryV1.registerSerializer(new PersonSerializerV1());

        Person deserializedWithV1 = deserialize(registryV1, bytesFromV3, Person.class);

        assertThat(deserializedWithV1.name).isEqualTo("Eve");
        assertThat(deserializedWithV1.age).isEqualTo(28);
        assertThat(deserializedWithV1.email).isNull();
        assertThat(deserializedWithV1.phone).isNull();
    }

    /**
     * Tests null value handling across versions.
     */
    @Test
    public void testNullHandlingAcrossVersions() {
        // V2 writes person with null email
        BinarySerializationRegistry registryV2 = new BinarySerializationRegistry();
        registryV2.registerSerializer(new PersonSerializerV2());

        Person personWithNullEmail = new Person("Frank", 45, null);
        byte[] bytes = serialize(registryV2, personWithNullEmail);

        // V1 reads it
        BinarySerializationRegistry registryV1 = new BinarySerializationRegistry();
        registryV1.registerSerializer(new PersonSerializerV1());

        Person deserialized = deserialize(registryV1, bytes, Person.class);

        assertThat(deserialized.name).isEqualTo("Frank");
        assertThat(deserialized.age).isEqualTo(45);
    }

    /**
     * Tests that reading same version produces identical results.
     */
    @Test
    public void testSameVersionRoundTrip() {
        BinarySerializationRegistry registry = new BinarySerializationRegistry();
        registry.registerSerializer(new PersonSerializerV2());

        Person original = new Person("Grace", 33, "grace@example.com");
        byte[] bytes = serialize(registry, original);
        Person deserialized = deserialize(registry, bytes, Person.class);

        assertThat(deserialized).isEqualTo(original);
    }

    // Helper methods

    private byte[] serialize(BinarySerializationRegistry registry, Object obj) {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(512);
        DefaultSerializationContext ctx = new DefaultSerializationContext(registry, buffer);
        ctx.writeObject(obj);
        return buffer.toByteArray();
    }

    private <T> T deserialize(BinarySerializationRegistry registry, byte[] bytes, Class<T> type) {
        CustomByteBuffer buffer = CustomByteBuffer.wrap(bytes);
        DefaultSerializationContext ctx = new DefaultSerializationContext(registry, buffer);
        return ctx.readObject();
    }

    // Test domain object

    private static class Person {
        final String name;
        final int age;
        final String email;
        final String phone;

        Person(String name, int age, String email) {
            this(name, age, email, null);
        }

        Person(String name, int age, String email, String phone) {
            this.name = name;
            this.age = age;
            this.email = email;
            this.phone = phone;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person person = (Person) o;
            return age == person.age &&
                   Objects.equals(name, person.name) &&
                   Objects.equals(email, person.email) &&
                   Objects.equals(phone, person.phone);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age, email, phone);
        }

        @Override
        public String toString() {
            return "Person{name='" + name + "', age=" + age +
                   ", email='" + email + "', phone='" + phone + "'}";
        }
    }

    // Version 1: Only name and age

    private static class PersonSerializerV1 implements SingleBinarySerializer<Person> {

        @Override
        public void write(SerializationContextWrite ctx, Person person) {
            ctx.writeUTF(person.name);
            ctx.writeInt(person.age);
        }

        @Override
        public Person read(SerializationContextRead ctx, byte version) {
            String name = ctx.readUTF();
            int age = ctx.readInt();
            return new Person(name, age, null);
        }

        @Override
        public Class<Person> javaClass() {
            return Person.class;
        }

        @Override
        public int type() {
            return TYPE_PERSON;
        }

        @Override
        public byte version() {
            return 1;
        }
    }

    // Version 2: Added email field

    private static class PersonSerializerV2 implements SingleBinarySerializer<Person> {

        @Override
        public void write(SerializationContextWrite ctx, Person person) {
            ctx.writeUTF(person.name);
            ctx.writeInt(person.age);

            // New field in version 2
            if (person.email != null) {
                ctx.writeBoolean(true);
                ctx.writeUTF(person.email);
            } else {
                ctx.writeBoolean(false);
            }
        }

        @Override
        public Person read(SerializationContextRead ctx, byte version) {
            String name = ctx.readUTF();
            int age = ctx.readInt();

            // Handle different versions
            String email = "unknown@example.com";
            if (version >= 2) {
                boolean hasEmail = ctx.readBoolean();
                if (hasEmail) {
                    email = ctx.readUTF();
                }
            }

            return new Person(name, age, email);
        }

        @Override
        public Class<Person> javaClass() {
            return Person.class;
        }

        @Override
        public int type() {
            return TYPE_PERSON;
        }

        @Override
        public byte version() {
            return 2;
        }
    }

    // Version 3: Added phone field

    private static class PersonSerializerV3 implements SingleBinarySerializer<Person> {

        @Override
        public void write(SerializationContextWrite ctx, Person person) {
            ctx.writeUTF(person.name);
            ctx.writeInt(person.age);

            // Email field (from version 2)
            if (person.email != null) {
                ctx.writeBoolean(true);
                ctx.writeUTF(person.email);
            } else {
                ctx.writeBoolean(false);
            }

            // New field in version 3
            if (person.phone != null) {
                ctx.writeBoolean(true);
                ctx.writeUTF(person.phone);
            } else {
                ctx.writeBoolean(false);
            }
        }

        @Override
        public Person read(SerializationContextRead ctx, byte version) {
            String name = ctx.readUTF();
            int age = ctx.readInt();

            // Email field (added in version 2)
            String email = "unknown@example.com";
            if (version >= 2) {
                boolean hasEmail = ctx.readBoolean();
                if (hasEmail) {
                    email = ctx.readUTF();
                }
            }

            // Phone field (added in version 3)
            String phone = "000-000-0000";
            if (version >= 3) {
                boolean hasPhone = ctx.readBoolean();
                if (hasPhone) {
                    phone = ctx.readUTF();
                }
            }

            return new Person(name, age, email, phone);
        }

        @Override
        public Class<Person> javaClass() {
            return Person.class;
        }

        @Override
        public int type() {
            return TYPE_PERSON;
        }

        @Override
        public byte version() {
            return 3;
        }
    }
}
