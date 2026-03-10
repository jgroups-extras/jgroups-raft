package org.jgroups.raft.util.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class CustomByteBuffer implements RandomAccessOutput {
    private ByteBuffer buffer;
    private int maxPosition;

    private CustomByteBuffer(int initialCapacity) {
        this.buffer = ByteBuffer.allocate(initialCapacity);
        this.maxPosition = 0;
    }

    /**
     * Creates a new buffer with default initial capacity (256 bytes).
     */
    public static CustomByteBuffer allocate() {
        return allocate(256);
    }

    /**
     * Creates a new buffer with specified initial capacity.
     *
     * @param capacity The initial capacity in bytes
     * @return A new CustomByteBuffer
     */
    public static CustomByteBuffer allocate(int capacity) {
        return new CustomByteBuffer(capacity);
    }

    /**
     * Wraps an existing byte array for reading.
     *
     * @param array The byte array to wrap
     * @return A new CustomByteBuffer wrapping the array
     */
    public static CustomByteBuffer wrap(byte[] array) {
        CustomByteBuffer buf = new CustomByteBuffer(0);
        buf.buffer = ByteBuffer.wrap(array);
        buf.maxPosition = array.length;
        return buf;
    }

    /**
     * Writes a single byte.
     */
    @Override
    public void writeByte(int value) {
        ensureWritable(1);
        buffer.put((byte) value);
        updateMaxPosition();
    }

    /**
     * Writes a short (2 bytes).
     */
    @Override
    public void writeShort(int value) {
        ensureWritable(2);
        buffer.putShort((short) value);
        updateMaxPosition();
    }

    /**
     * Writes an int (4 bytes).
     */
    @Override
    public void writeInt(int value) {
        ensureWritable(4);
        buffer.putInt(value);
        updateMaxPosition();
    }

    @Override
    public void write(int i) {
        writeInt(i);
    }

    @Override
    public void writeBytes(String s) {
        writeUTF(s);
    }

    @Override
    public void writeChar(int i) {
        ensureWritable(Character.BYTES);
        buffer.putChar((char) i);
        updateMaxPosition();
    }

    @Override
    public void writeChars(String s) {
        writeUTF(s);
    }

    /**
     * Writes a long (8 bytes).
     */
    @Override
    public void writeLong(long value) {
        ensureWritable(8);
        buffer.putLong(value);
        updateMaxPosition();
    }

    /**
     * Writes a boolean (1 byte).
     */
    @Override
    public void writeBoolean(boolean value) {
        writeByte(value ? 1 : 0);
    }

    /**
     * Writes a float (4 bytes).
     */
    @Override
    public void writeFloat(float value) {
        ensureWritable(4);
        buffer.putFloat(value);
        updateMaxPosition();
    }

    /**
     * Writes a double (8 bytes).
     */
    @Override
    public void writeDouble(double value) {
        ensureWritable(8);
        buffer.putDouble(value);
        updateMaxPosition();
    }

    /**
     * Writes a byte array.
     */
    @Override
    public void write(byte[] src) {
        ensureWritable(src.length);
        buffer.put(src);
        updateMaxPosition();
    }

    /**
     * Writes a portion of a byte array.
     */
    @Override
    public void write(byte[] src, int offset, int length) {
        ensureWritable(length);
        buffer.put(src, offset, length);
        updateMaxPosition();
    }

    /**
     * Writes a string in UTF-8 encoding with a length prefix.
     *
     * <p>
     * Format: [length: short][utf8-bytes]
     * Maximum string length is 32KB (Short.MAX_VALUE).
     * </p>
     *
     * @param s The string to write
     * @throws IllegalArgumentException If the UTF-8 encoded string exceeds 32KB
     */
    @Override
    public void writeUTF(String s) {
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        if (utf8.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("String too long for UTF encoding: " + utf8.length);
        }
        writeShort(utf8.length);
        write(utf8);
    }

    /**
     * Reads a single byte.
     */
    public byte readByte() {
        return buffer.get();
    }

    /**
     * Reads a short (2 bytes).
     */
    public short readShort() {
        return buffer.getShort();
    }

    /**
     * Reads an int (4 bytes).
     */
    public int readInt() {
        return buffer.getInt();
    }

    /**
     * Reads a long (8 bytes).
     */
    public long readLong() {
        return buffer.getLong();
    }

    /**
     * Reads a boolean (1 byte).
     */
    public boolean readBoolean() {
        return buffer.get() != 0;
    }

    /**
     * Reads a float (4 bytes).
     */
    public float readFloat() {
        return buffer.getFloat();
    }

    /**
     * Reads a double (8 bytes).
     */
    public double readDouble() {
        return buffer.getDouble();
    }

    /**
     * Reads bytes into a new array.
     *
     * @param length The number of bytes to read
     * @return A new byte array containing the read bytes
     */
    public byte[] readBytes(int length) {
        byte[] dst = new byte[length];
        buffer.get(dst);
        return dst;
    }

    /**
     * Reads bytes into an existing array.
     */
    public void readBytes(byte[] dst) {
        buffer.get(dst);
    }

    /**
     * Reads bytes into a portion of an existing array.
     */
    public void readBytes(byte[] dst, int offset, int length) {
        buffer.get(dst, offset, length);
    }

    /**
     * Reads a UTF-8 string written with {@link #writeUTF(String)}.
     */
    public String readUTF() {
        short length = readShort();
        byte[] utf8 = readBytes(length);
        return new String(utf8, StandardCharsets.UTF_8);
    }

    /**
     * Returns the current position in the buffer.
     */
    public int position() {
        return buffer.position();
    }

    /**
     * Sets the position in the buffer.
     */
    public void position(int newPosition) {
        buffer.position(newPosition);
    }

    /**
     * Skips n bytes.
     *
     * @param bytes number of bytes to skip.
     * @return this instance.
     */
    public CustomByteBuffer skip(int bytes) {
        buffer.position(buffer.position() + bytes);
        return this;
    }

    /**
     * Returns the number of bytes that can be read from the current position.
     */
    public int readableBytes() {
        return buffer.remaining();
    }

    /**
     * Returns the capacity of the underlying buffer.
     */
    public int capacity() {
        return buffer.capacity();
    }

    /**
     * Flips the buffer from write mode to read mode.
     *
     * <p>
     * Sets the limit to the maximum position written and resets the position to zero.
     * Call this after writing, before reading.
     * </p>
     */
    public CustomByteBuffer flip() {
        buffer.limit(maxPosition);
        buffer.position(0);
        return this;
    }

    /**
     * Clears the buffer for reuse.
     *
     * <p>
     * Sets the position to zero and the limit to capacity.
     * </p>
     */
    public CustomByteBuffer clear() {
        buffer.clear();
        maxPosition = 0;
        return this;
    }

    /**
     * Returns a copy of the written bytes.
     *
     * <p>
     * This creates a new byte array containing all bytes from index 0 to the current position.
     * The buffer's position is not affected.
     * </p>
     */
    public byte[] toByteArray() {
        byte[] result = new byte[buffer.position()];
        buffer.duplicate().flip().get(result);
        return result;
    }

    /**
     * Updates the maximum position tracker after a write operation.
     */
    private void updateMaxPosition() {
        maxPosition = Math.max(maxPosition, buffer.position());
    }

    /**
     * Ensures the buffer has capacity to write at least the specified number of bytes.
     *
     * <p>
     * If the remaining capacity is insufficient, the buffer is grown. The new capacity
     * is the larger of:
     * <ul>
     *   <li>Double the current capacity</li>
     *   <li>Current capacity plus the required additional bytes</li>
     * </ul>
     * </p>
     *
     * @param minWritableBytes The minimum number of bytes that must be writable
     */
    private void ensureWritable(int minWritableBytes) {
        int writableBytes = buffer.remaining();
        if (writableBytes >= minWritableBytes) {
            return;
        }

        // Calculate new capacity
        int currentCapacity = buffer.capacity();
        int neededCapacity = currentCapacity + minWritableBytes - writableBytes;
        int newCapacity = Math.max(currentCapacity * 2, neededCapacity);

        // Create new buffer and copy existing data
        ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
        int currentPosition = buffer.position();
        buffer.flip();
        newBuffer.put(buffer);
        buffer = newBuffer;

        // Preserve maxPosition after buffer growth
        maxPosition = Math.max(maxPosition, currentPosition);
    }
}
