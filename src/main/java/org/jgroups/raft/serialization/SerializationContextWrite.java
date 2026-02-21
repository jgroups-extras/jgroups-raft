package org.jgroups.raft.serialization;

import org.jgroups.raft.internal.serialization.binary.AbstractSerializationContextWrite;

/**
 * Context for writing objects and primitives during serialization.
 *
 * <p>
 * This context is provided to {@link JGroupsRaftCustomMarshaller#write(SerializationContextWrite, Object)} and offers
 * methods to write primitives and nested objects to the output stream.
 * </p>
 *
 * <h2>Writing Primitives</h2>
 *
 * <p>
 * Use the provided write methods for primitive types and strings:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public void write(SerializationContextWrite ctx, MyObject obj) {
 *     ctx.writeByte(obj.getByteValue());
 *     ctx.writeShort(obj.getShortValue());
 *     ctx.writeInt(obj.getIntValue());
 *     ctx.writeLong(obj.getLongValue());
 *     ctx.writeBoolean(obj.getBooleanValue());
 *     ctx.writeFloat(obj.getFloatValue());
 *     ctx.writeDouble(obj.getDoubleValue());
 *     ctx.writeUTF(obj.getStringValue());
 *     ctx.writeBytes(obj.getByteArray());
 * }
 * }</pre>
 *
 * <h2>Writing Nested Objects</h2>
 *
 * <p>
 * Use {@link #writeObject(Object)} to write nested objects. The framework will automatically invoke the appropriate serializer
 * for the object's type:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public void write(SerializationContextWrite ctx, Order order) {
 *     ctx.writeObject(order.getCustomer());  // Person object
 *     ctx.writeObject(order.getShippingAddress());  // Address object
 *     ctx.writeDouble(order.getTotal());
 * }
 * }</pre>
 *
 * <h2>Null Values</h2>
 *
 * <p>
 * {@link #writeObject(Object)} handles null values automatically. You don't need to write a null flag manually:
 * </p>
 *
 * <pre>{@code
 * // DON'T do this:
 * ctx.writeBoolean(obj.getAddress() != null);
 * if (obj.getAddress() != null) {
 *     ctx.writeObject(obj.getAddress());
 * }
 *
 * // DO this instead:
 * ctx.writeObject(obj.getAddress());  // Null is handled automatically
 * }</pre>
 *
 * <h2>Byte Arrays</h2>
 *
 * <p>
 * For byte arrays, use {@link #writeBytes(byte[])} or {@link #writeBytes(byte[], int, int)} for partial arrays:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public void write(SerializationContextWrite ctx, MyObject obj) {
 *     byte[] data = obj.getData();
 *     ctx.writeInt(data.length);  // Write length prefix
 *     ctx.writeBytes(data);       // Write the bytes
 * }
 * }</pre>
 *
 * <h2>Strings</h2>
 *
 * <p>
 * {@link #writeUTF(String)} writes strings in UTF-8 encoding with a length prefix. Maximum string length is 32KB
 * (Short.MAX_VALUE bytes after UTF-8 encoding):
 * </p>
 *
 * <pre>{@code
 * ctx.writeUTF(obj.getName());  // Automatically length-prefixed
 * }</pre>
 *
 * <p>
 * For strings longer than 32KB, write them as byte arrays:
 * </p>
 *
 * <pre>{@code
 * byte[] utf8 = largeString.getBytes(StandardCharsets.UTF_8);
 * ctx.writeInt(utf8.length);
 * ctx.writeBytes(utf8);
 * }</pre>
 *
 * <h2>Write Order</h2>
 *
 * <p>
 * <b>Critical:</b> The order in which you write fields must exactly match the order in which you read them in
 * {@link JGroupsRaftCustomMarshaller#read(SerializationContextRead, byte)}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see SerializationContextRead
 * @see JGroupsRaftCustomMarshaller#write(SerializationContextWrite, Object)
 */
public sealed interface SerializationContextWrite permits AbstractSerializationContextWrite {

    /**
     * Writes an object with automatic type identification.
     *
     * <p>
     * The object's type is determined, its type ID is written, and the appropriate serializer is invoked. Null values
     * are handled automatically.
     * </p>
     *
     * @param obj The object to write (can be null)
     * @throws IllegalStateException if no serializer is registered for the object's type
     */
    void writeObject(Object obj);

    /**
     * Writes a single byte.
     *
     * @param v The byte value (only the lowest 8 bits are written)
     */
    void writeByte(int v);

    /**
     * Writes a short (2 bytes).
     *
     * @param v The short value
     */
    void writeShort(int v);

    /**
     * Writes an int (4 bytes).
     *
     * @param v The int value
     */
    void writeInt(int v);

    /**
     * Writes a long (8 bytes).
     *
     * @param v The long value
     */
    void writeLong(long v);

    /**
     * Writes a boolean (1 byte).
     *
     * @param v The boolean value
     */
    void writeBoolean(boolean v);

    /**
     * Writes a float (4 bytes).
     *
     * @param v The float value
     */
    void writeFloat(float v);

    /**
     * Writes a double (8 bytes).
     *
     * @param v The double value
     */
    void writeDouble(double v);

    /**
     * Writes a string in UTF-8 encoding with a length prefix.
     *
     * <p>
     * Maximum string length is 32KB (Short.MAX_VALUE bytes after UTF-8 encoding).
     * </p>
     *
     * @param s The string to write
     * @throws IllegalArgumentException if the UTF-8 encoded string exceeds 32KB
     */
    void writeUTF(String s);

    /**
     * Writes all bytes from the array.
     *
     * @param b The byte array to write
     */
    void writeBytes(byte[] b);

    /**
     * Writes a portion of a byte array.
     *
     * @param b The byte array
     * @param off The starting offset in the array
     * @param len The number of bytes to write
     */
    void writeBytes(byte[] b, int off, int len);

}
