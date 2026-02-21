package org.jgroups.raft.serialization;

import org.jgroups.raft.internal.serialization.binary.DefaultSerializationContext;

/**
 * Context for reading objects and primitives during deserialization.
 *
 * <p>
 * This context is provided to {@link JGroupsRaftCustomMarshaller#read(SerializationContextRead, byte)} and offers methods
 * to read primitives and nested objects from the input stream.
 * </p>
 *
 * <h2>Reading Primitives</h2>
 *
 * <p>
 * Use the provided read methods for primitive types and strings, in the same order they were written:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public MyObject read(SerializationContextRead ctx, byte version) {
 *     byte byteValue = ctx.readByte();
 *     short shortValue = ctx.readShort();
 *     int intValue = ctx.readInt();
 *     long longValue = ctx.readLong();
 *     boolean booleanValue = ctx.readBoolean();
 *     float floatValue = ctx.readFloat();
 *     double doubleValue = ctx.readDouble();
 *     String stringValue = ctx.readUTF();
 *     byte[] byteArray = ctx.readBytes(intValue);  // Assuming intValue is length
 *
 *     return new MyObject(byteValue, shortValue, intValue, longValue,
 *                        booleanValue, floatValue, doubleValue, stringValue, byteArray);
 * }
 * }</pre>
 *
 * <h2>Reading Nested Objects</h2>
 *
 * <p>
 * Use {@link #readObject()} to read nested objects. The framework will automatically determine the object's type and invoke
 * the appropriate deserializer:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public Order read(SerializationContextRead ctx, byte version) {
 *     Person customer = ctx.readObject();  // Automatically deserializes Person
 *     Address address = ctx.readObject();  // Automatically deserializes Address
 *     double total = ctx.readDouble();
 *     return new Order(customer, address, total);
 * }
 * }</pre>
 *
 * <h2>Null Values</h2>
 *
 * <p>
 * {@link #readObject()} may return null if a null value was written:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public Person read(SerializationContextRead ctx, byte version) {
 *     String name = ctx.readUTF();
 *     Address address = ctx.readObject();  // May be null
 *     return new Person(name, address);
 * }
 * }</pre>
 *
 * <h2>Byte Arrays</h2>
 *
 * <p>
 * For byte arrays, read the length first, then the bytes:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public MyObject read(SerializationContextRead ctx, byte version) {
 *     int length = ctx.readInt();  // Read length prefix
 *     byte[] data = ctx.readBytes(length);  // Read the bytes
 *     return new MyObject(data);
 * }
 * }</pre>
 *
 * <p>
 * Alternatively, read into an existing array:
 * </p>
 *
 * <pre>{@code
 * byte[] buffer = new byte[1024];
 * ctx.readBytes(buffer);  // Reads exactly 1024 bytes
 * }</pre>
 *
 * <h2>Read Order</h2>
 *
 * <p>
 * <b>Critical:</b> The order in which you read fields must exactly match the order in which they were written in
 * {@link JGroupsRaftCustomMarshaller#write(SerializationContextWrite, Object)}.
 * </p>
 *
 * <pre>{@code
 * // Write order:
 * ctx.writeUTF(name);
 * ctx.writeInt(age);
 *
 * // Read order MUST match:
 * String name = ctx.readUTF();
 * int age = ctx.readInt();
 * }</pre>
 *
 * @since 2.0
 * @author José Bolina
 * @see SerializationContextWrite
 * @see JGroupsRaftCustomMarshaller#read(SerializationContextRead, byte)
 */
public sealed interface SerializationContextRead permits DefaultSerializationContext {

    /**
     * Reads an object with automatic type identification.
     *
     * <p>
     * The object's type ID is read, the appropriate deserializer is determined, and the object is deserialized. May return
     * {@code null} if a null value was written.
     * </p>
     *
     * @param <T> The expected type
     * @return The deserialized object (can be null)
     * @throws IllegalStateException if no serializer is registered for the type ID
     */
    <T> T readObject();

    /**
     * Reads a single byte.
     *
     * @return The byte value
     */
    byte readByte();

    /**
     * Reads a short (2 bytes).
     *
     * @return The short value
     */
    short readShort();

    /**
     * Reads an int (4 bytes).
     *
     * @return The int value
     */
    int readInt();

    /**
     * Reads a long (8 bytes).
     *
     * @return The long value
     */
    long readLong();

    /**
     * Reads a boolean (1 byte).
     *
     * @return The boolean value
     */
    boolean readBoolean();

    /**
     * Reads a float (4 bytes).
     *
     * @return The float value
     */
    float readFloat();

    /**
     * Reads a double (8 bytes).
     *
     * @return The double value
     */
    double readDouble();

    /**
     * Reads a UTF-8 encoded string with length prefix.
     *
     * @return The string value
     */
    String readUTF();

    /**
     * Reads bytes into a new array.
     *
     * @param length The number of bytes to read
     * @return A new byte array containing the read bytes
     */
    byte[] readBytes(int length);

    /**
     * Reads bytes into an existing array.
     *
     * <p>
     * Reads exactly {@code b.length} bytes from the stream.
     * </p>
     *
     * @param b The array to read into
     */
    void readBytes(byte[] b);

    /**
     * Reads bytes into a portion of an existing array.
     *
     * @param b The array to read into
     * @param off The starting offset in the array
     * @param len The number of bytes to read
     */
    void readBytes(byte[] b, int off, int len);
}
