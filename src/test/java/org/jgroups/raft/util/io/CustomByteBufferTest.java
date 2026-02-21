package org.jgroups.raft.util.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;

import java.nio.BufferUnderflowException;

import org.testng.annotations.Test;

/**
 * Tests for {@link CustomByteBuffer} to ensure read/write operations work correctly.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CustomByteBufferTest {

    @Test
    public void testByteReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(16);

        buffer.writeByte(0x42);
        buffer.writeByte(0xFF);
        buffer.writeByte(0x00);

        buffer.flip();

        assertThat(buffer.readByte()).isEqualTo((byte) 0x42);
        assertThat(buffer.readByte()).isEqualTo((byte) 0xFF);
        assertThat(buffer.readByte()).isEqualTo((byte) 0x00);
    }

    @Test
    public void testShortReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(16);

        buffer.writeShort(1000);
        buffer.writeShort(-1000);
        buffer.writeShort(Short.MAX_VALUE);
        buffer.writeShort(Short.MIN_VALUE);

        buffer.flip();

        assertThat(buffer.readShort()).isEqualTo((short) 1000);
        assertThat(buffer.readShort()).isEqualTo((short) -1000);
        assertThat(buffer.readShort()).isEqualTo(Short.MAX_VALUE);
        assertThat(buffer.readShort()).isEqualTo(Short.MIN_VALUE);
    }

    @Test
    public void testIntReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(32);

        buffer.writeInt(42);
        buffer.writeInt(-42);
        buffer.writeInt(Integer.MAX_VALUE);
        buffer.writeInt(Integer.MIN_VALUE);
        buffer.writeInt(0);

        buffer.flip();

        assertThat(buffer.readInt()).isEqualTo(42);
        assertThat(buffer.readInt()).isEqualTo(-42);
        assertThat(buffer.readInt()).isEqualTo(Integer.MAX_VALUE);
        assertThat(buffer.readInt()).isEqualTo(Integer.MIN_VALUE);
        assertThat(buffer.readInt()).isEqualTo(0);
    }

    @Test
    public void testLongReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeLong(123456789L);
        buffer.writeLong(-123456789L);
        buffer.writeLong(Long.MAX_VALUE);
        buffer.writeLong(Long.MIN_VALUE);
        buffer.writeLong(0L);

        buffer.flip();

        assertThat(buffer.readLong()).isEqualTo(123456789L);
        assertThat(buffer.readLong()).isEqualTo(-123456789L);
        assertThat(buffer.readLong()).isEqualTo(Long.MAX_VALUE);
        assertThat(buffer.readLong()).isEqualTo(Long.MIN_VALUE);
        assertThat(buffer.readLong()).isEqualTo(0L);
    }

    @Test
    public void testBooleanReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(16);

        buffer.writeBoolean(true);
        buffer.writeBoolean(false);
        buffer.writeBoolean(true);

        buffer.flip();

        assertThat(buffer.readBoolean()).isTrue();
        assertThat(buffer.readBoolean()).isFalse();
        assertThat(buffer.readBoolean()).isTrue();
    }

    @Test
    public void testFloatReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(32);

        buffer.writeFloat(3.14159f);
        buffer.writeFloat(-3.14159f);
        buffer.writeFloat(Float.MAX_VALUE);
        buffer.writeFloat(Float.MIN_VALUE);
        buffer.writeFloat(0.0f);

        buffer.flip();

        assertThat(buffer.readFloat()).isEqualTo(3.14159f);
        assertThat(buffer.readFloat()).isEqualTo(-3.14159f);
        assertThat(buffer.readFloat()).isEqualTo(Float.MAX_VALUE);
        assertThat(buffer.readFloat()).isEqualTo(Float.MIN_VALUE);
        assertThat(buffer.readFloat()).isEqualTo(0.0f);
    }

    @Test
    public void testDoubleReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeDouble(3.141592653589793);
        buffer.writeDouble(-3.141592653589793);
        buffer.writeDouble(Double.MAX_VALUE);
        buffer.writeDouble(Double.MIN_VALUE);
        buffer.writeDouble(0.0);

        buffer.flip();

        assertThat(buffer.readDouble()).isEqualTo(3.141592653589793);
        assertThat(buffer.readDouble()).isEqualTo(-3.141592653589793);
        assertThat(buffer.readDouble()).isEqualTo(Double.MAX_VALUE);
        assertThat(buffer.readDouble()).isEqualTo(Double.MIN_VALUE);
        assertThat(buffer.readDouble()).isEqualTo(0.0);
    }

    @Test
    public void testUTFReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(256);

        buffer.writeUTF("Hello, World!");
        buffer.writeUTF("");
        buffer.writeUTF("Special chars: 日本語 🎉");

        buffer.flip();

        assertThat(buffer.readUTF()).isEqualTo("Hello, World!");
        assertThat(buffer.readUTF()).isEqualTo("");
        assertThat(buffer.readUTF()).isEqualTo("Special chars: 日本語 🎉");
    }

    @Test
    public void testUTFMaxLength() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(40000);

        // Create a string that's close to the 32KB limit
        String longString = "a".repeat(10000);

        buffer.writeUTF(longString);
        buffer.flip();

        assertThat(buffer.readUTF()).isEqualTo(longString);
    }

    @Test
    public void testUTFTooLong() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(100000);

        // Create a string that exceeds 32KB when UTF-8 encoded
        String tooLongString = "a".repeat(Short.MAX_VALUE + 1);

        assertThatThrownBy(() -> buffer.writeUTF(tooLongString))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String too long for UTF encoding");
    }

    @Test
    public void testBytesReadWrite() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        byte[] data1 = {1, 2, 3, 4, 5};
        byte[] data2 = {10, 20, 30};

        buffer.write(data1);
        buffer.write(data2);

        buffer.flip();

        byte[] read1 = buffer.readBytes(5);
        byte[] read2 = buffer.readBytes(3);

        assertThat(read1).containsExactly(1, 2, 3, 4, 5);
        assertThat(read2).containsExactly(10, 20, 30);
    }

    @Test
    public void testBytesReadWriteWithOffset() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        byte[] source = {1, 2, 3, 4, 5, 6, 7, 8};

        // Write middle portion
        buffer.write(source, 2, 4);

        buffer.flip();

        byte[] read = buffer.readBytes(4);
        assertThat(read).containsExactly(3, 4, 5, 6);
    }

    @Test
    public void testReadBytesIntoExistingArray() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        byte[] source = {1, 2, 3, 4, 5};
        buffer.write(source);

        buffer.flip();

        byte[] destination = new byte[5];
        buffer.readBytes(destination);

        assertThat(destination).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testReadBytesIntoExistingArrayWithOffset() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        byte[] source = {10, 20, 30};
        buffer.write(source);

        buffer.flip();

        byte[] destination = new byte[10];
        buffer.readBytes(destination, 3, 3);

        assertThat(destination[0]).isEqualTo((byte) 0);
        assertThat(destination[1]).isEqualTo((byte) 0);
        assertThat(destination[2]).isEqualTo((byte) 0);
        assertThat(destination[3]).isEqualTo((byte) 10);
        assertThat(destination[4]).isEqualTo((byte) 20);
        assertThat(destination[5]).isEqualTo((byte) 30);
    }

    @Test
    public void testBufferGrowth() {
        // Start with small buffer
        CustomByteBuffer buffer = CustomByteBuffer.allocate(8);

        // Write more data than initial capacity
        for (int i = 0; i < 100; i++) {
            buffer.writeInt(i);
        }

        assertThat(buffer.position()).isEqualTo(400); // 100 ints * 4 bytes

        buffer.flip();

        // Read back all data
        for (int i = 0; i < 100; i++) {
            assertThat(buffer.readInt()).isEqualTo(i);
        }
    }

    @Test
    public void testPositionAndSkip() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeInt(1);
        buffer.writeInt(2);
        buffer.writeInt(3);

        assertThat(buffer.position()).isEqualTo(12);

        buffer.flip();

        // Read first int
        assertThat(buffer.readInt()).isEqualTo(1);

        // Skip second int
        buffer.skip(4);

        // Read third int
        assertThat(buffer.readInt()).isEqualTo(3);
    }

    @Test
    public void testPositionSetter() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeInt(100);
        buffer.writeInt(200);
        buffer.writeInt(300);

        // Rewind to position 4 (second int)
        buffer.position(4);
        buffer.writeInt(999);

        buffer.flip();

        assertThat(buffer.readInt()).isEqualTo(100);
        assertThat(buffer.readInt()).isEqualTo(999); // Modified
        assertThat(buffer.readInt()).isEqualTo(300);
    }

    @Test
    public void testReadableBytes() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeInt(1);
        buffer.writeInt(2);
        buffer.writeInt(3);

        buffer.flip();

        assertThat(buffer.readableBytes()).isEqualTo(12);

        buffer.readInt();
        assertThat(buffer.readableBytes()).isEqualTo(8);

        buffer.readInt();
        assertThat(buffer.readableBytes()).isEqualTo(4);

        buffer.readInt();
        assertThat(buffer.readableBytes()).isEqualTo(0);
    }

    @Test
    public void testCapacity() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(256);
        assertThat(buffer.capacity()).isEqualTo(256);

        // Write data to trigger growth
        for (int i = 0; i < 100; i++) {
            buffer.writeInt(i);
        }

        // Capacity should have grown
        assertThat(buffer.capacity()).isGreaterThan(256);
    }

    @Test
    public void testClearAndReuse() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeInt(123);
        buffer.clear();

        assertThat(buffer.position()).isEqualTo(0);

        buffer.writeInt(456);
        buffer.flip();

        assertThat(buffer.readInt()).isEqualTo(456);
    }

    @Test
    public void testToByteArray() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeInt(1);
        buffer.writeInt(2);
        buffer.writeInt(3);

        byte[] bytes = buffer.toByteArray();

        assertThat(bytes).hasSize(12); // 3 ints * 4 bytes
    }

    @Test
    public void testWrapByteArray() {
        byte[] source = {1, 2, 3, 4, 5};
        CustomByteBuffer buffer = CustomByteBuffer.wrap(source);

        assertThat(buffer.readByte()).isEqualTo((byte) 1);
        assertThat(buffer.readByte()).isEqualTo((byte) 2);
        assertThat(buffer.readByte()).isEqualTo((byte) 3);
        assertThat(buffer.readByte()).isEqualTo((byte) 4);
        assertThat(buffer.readByte()).isEqualTo((byte) 5);
    }

    @Test
    public void testUnderflow() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(4);
        buffer.writeInt(123);
        buffer.flip();

        buffer.readInt(); // Read the only int

        // Try to read beyond available data
        assertThatThrownBy(() -> buffer.readInt())
            .isInstanceOf(BufferUnderflowException.class);
    }

    @Test
    public void testMixedOperations() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(256);

        // Write mixed data types
        buffer.writeByte(1);
        buffer.writeShort(2);
        buffer.writeInt(3);
        buffer.writeLong(4L);
        buffer.writeFloat(5.0f);
        buffer.writeDouble(6.0);
        buffer.writeBoolean(true);
        buffer.writeUTF("test");
        buffer.write(new byte[]{7, 8, 9});

        buffer.flip();

        // Read back in same order
        assertThat(buffer.readByte()).isEqualTo((byte) 1);
        assertThat(buffer.readShort()).isEqualTo((short) 2);
        assertThat(buffer.readInt()).isEqualTo(3);
        assertThat(buffer.readLong()).isEqualTo(4L);
        assertThat(buffer.readFloat()).isEqualTo(5.0f);
        assertThat(buffer.readDouble()).isEqualTo(6.0);
        assertThat(buffer.readBoolean()).isTrue();
        assertThat(buffer.readUTF()).isEqualTo("test");
        assertThat(buffer.readBytes(3)).containsExactly(7, 8, 9);
    }

    @Test
    public void testEmptyByteArray() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(16);

        byte[] empty = {};
        buffer.write(empty);

        buffer.flip();

        byte[] read = buffer.readBytes(0);
        assertThat(read).isEmpty();
    }

    @Test
    public void testZeroValues() {
        CustomByteBuffer buffer = CustomByteBuffer.allocate(64);

        buffer.writeByte(0);
        buffer.writeShort(0);
        buffer.writeInt(0);
        buffer.writeLong(0L);
        buffer.writeFloat(0.0f);
        buffer.writeDouble(0.0);
        buffer.writeBoolean(false);

        buffer.flip();

        assertThat(buffer.readByte()).isEqualTo((byte) 0);
        assertThat(buffer.readShort()).isEqualTo((short) 0);
        assertThat(buffer.readInt()).isEqualTo(0);
        assertThat(buffer.readLong()).isEqualTo(0L);
        assertThat(buffer.readFloat()).isEqualTo(0.0f);
        assertThat(buffer.readDouble()).isEqualTo(0.0);
        assertThat(buffer.readBoolean()).isFalse();
    }
}
