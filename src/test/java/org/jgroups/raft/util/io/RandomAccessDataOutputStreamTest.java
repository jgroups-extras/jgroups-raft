package org.jgroups.raft.util.io;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.util.ByteArrayDataOutputStream;

import java.io.IOException;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RandomAccessDataOutputStreamTest {

    @Test
    public void testPositionReturnsCurrentWritePosition() {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(64);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        assertThat(adapter.position()).isEqualTo(0);
    }

    @Test
    public void testPositionAdvancesAfterWrites() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(64);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeInt(42);
        assertThat(adapter.position()).isEqualTo(4);

        adapter.writeLong(100L);
        assertThat(adapter.position()).isEqualTo(12);

        adapter.writeBoolean(true);
        assertThat(adapter.position()).isEqualTo(13);
    }

    @Test
    public void testSetPositionChangesWriteLocation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(64);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeInt(1111);
        adapter.writeInt(2222);
        adapter.writeInt(3333);

        // Position should be at 12 (3 ints)
        assertThat(adapter.position()).isEqualTo(12);

        // Move back to position 4 (second int)
        adapter.position(4);
        assertThat(adapter.position()).isEqualTo(4);

        // Overwrite second int
        adapter.writeInt(9999);

        // Position should now be at 8
        assertThat(adapter.position()).isEqualTo(8);

        // Read back the buffer to verify
        byte[] buffer = baos.buffer();
        int firstInt = ((buffer[0] & 0xFF) << 24) | ((buffer[1] & 0xFF) << 16) |
                       ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF);
        int secondInt = ((buffer[4] & 0xFF) << 24) | ((buffer[5] & 0xFF) << 16) |
                        ((buffer[6] & 0xFF) << 8) | (buffer[7] & 0xFF);
        int thirdInt = ((buffer[8] & 0xFF) << 24) | ((buffer[9] & 0xFF) << 16) |
                       ((buffer[10] & 0xFF) << 8) | (buffer[11] & 0xFF);

        assertThat(firstInt).isEqualTo(1111);
        assertThat(secondInt).isEqualTo(9999); // Patched value
        assertThat(thirdInt).isEqualTo(3333);
    }

    @Test
    public void testPatchLengthFieldPattern() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(64);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        // Reserve space for length (common serialization pattern)
        int lengthPosition = adapter.position();
        adapter.writeInt(0); // Placeholder

        int dataStart = adapter.position();

        // Write variable-length data
        adapter.writeInt(100);
        adapter.writeInt(200);
        adapter.writeInt(300);

        int dataEnd = adapter.position();
        int dataLength = dataEnd - dataStart;

        // Patch the length field
        int savedPosition = adapter.position();
        adapter.position(lengthPosition);
        adapter.writeInt(dataLength);
        adapter.position(savedPosition);

        // Verify
        byte[] buffer = baos.buffer();

        // Read length field (first 4 bytes)
        int length = ((buffer[0] & 0xFF) << 24) | ((buffer[1] & 0xFF) << 16) |
                     ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF);

        assertThat(length).isEqualTo(12); // 3 ints = 12 bytes
        assertThat(adapter.position()).isEqualTo(16); // 4 (length) + 12 (data)
    }

    @Test
    public void testWriteByteDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(16);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeByte(0x42);

        assertThat(baos.buffer()[0]).isEqualTo((byte) 0x42);
        assertThat(adapter.position()).isEqualTo(1);
    }

    @Test
    public void testWriteShortDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(16);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeShort(1000);

        byte[] buffer = baos.buffer();
        short value = (short) (((buffer[0] & 0xFF) << 8) | (buffer[1] & 0xFF));

        assertThat(value).isEqualTo((short) 1000);
        assertThat(adapter.position()).isEqualTo(2);
    }

    @Test
    public void testWriteIntDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(16);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeInt(123456);

        byte[] buffer = baos.buffer();
        int value = ((buffer[0] & 0xFF) << 24) | ((buffer[1] & 0xFF) << 16) |
                    ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF);

        assertThat(value).isEqualTo(123456);
        assertThat(adapter.position()).isEqualTo(4);
    }

    @Test
    public void testWriteLongDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(16);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeLong(123456789012345L);

        byte[] buffer = baos.buffer();
        long value = ((long)(buffer[0] & 0xFF) << 56) |
                     ((long)(buffer[1] & 0xFF) << 48) |
                     ((long)(buffer[2] & 0xFF) << 40) |
                     ((long)(buffer[3] & 0xFF) << 32) |
                     ((long)(buffer[4] & 0xFF) << 24) |
                     ((long)(buffer[5] & 0xFF) << 16) |
                     ((long)(buffer[6] & 0xFF) << 8) |
                     ((long)(buffer[7] & 0xFF));

        assertThat(value).isEqualTo(123456789012345L);
        assertThat(adapter.position()).isEqualTo(8);
    }

    @Test
    public void testWriteBooleanDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(16);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeBoolean(true);
        adapter.writeBoolean(false);

        byte[] buffer = baos.buffer();

        assertThat(buffer[0]).isEqualTo((byte) 1);
        assertThat(buffer[1]).isEqualTo((byte) 0);
        assertThat(adapter.position()).isEqualTo(2);
    }

    @Test
    public void testWriteFloatDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(16);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeFloat(3.14159f);

        byte[] buffer = baos.buffer();
        int bits = ((buffer[0] & 0xFF) << 24) | ((buffer[1] & 0xFF) << 16) |
                   ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF);
        float value = Float.intBitsToFloat(bits);

        assertThat(value).isEqualTo(3.14159f);
        assertThat(adapter.position()).isEqualTo(4);
    }

    @Test
    public void testWriteDoubleDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(16);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeDouble(2.718281828);

        byte[] buffer = baos.buffer();
        long bits = ((long)(buffer[0] & 0xFF) << 56) |
                    ((long)(buffer[1] & 0xFF) << 48) |
                    ((long)(buffer[2] & 0xFF) << 40) |
                    ((long)(buffer[3] & 0xFF) << 32) |
                    ((long)(buffer[4] & 0xFF) << 24) |
                    ((long)(buffer[5] & 0xFF) << 16) |
                    ((long)(buffer[6] & 0xFF) << 8) |
                    ((long)(buffer[7] & 0xFF));
        double value = Double.longBitsToDouble(bits);

        assertThat(value).isEqualTo(2.718281828);
        assertThat(adapter.position()).isEqualTo(8);
    }

    @Test
    public void testWriteByteArrayDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(32);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        byte[] data = {1, 2, 3, 4, 5};
        adapter.write(data);

        byte[] buffer = baos.buffer();

        for (int i = 0; i < data.length; i++) {
            assertThat(buffer[i]).isEqualTo(data[i]);
        }
        assertThat(adapter.position()).isEqualTo(5);
    }

    @Test
    public void testWriteByteArrayWithOffsetDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(32);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};
        adapter.write(data, 2, 4); // Write bytes 2, 3, 4, 5

        byte[] buffer = baos.buffer();

        assertThat(buffer[0]).isEqualTo((byte) 3);
        assertThat(buffer[1]).isEqualTo((byte) 4);
        assertThat(buffer[2]).isEqualTo((byte) 5);
        assertThat(buffer[3]).isEqualTo((byte) 6);
        assertThat(adapter.position()).isEqualTo(4);
    }

    @Test
    public void testWriteUTFDelegation() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(64);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        adapter.writeUTF("Hello");

        // ByteArrayDataOutputStream.writeUTF follows DataOutput spec:
        // 2 bytes for length, then UTF-8 bytes
        byte[] buffer = baos.buffer();
        int length = ((buffer[0] & 0xFF) << 8) | (buffer[1] & 0xFF);

        assertThat(length).isEqualTo(5); // "Hello" = 5 bytes
        assertThat(adapter.position()).isEqualTo(7); // 2 (length) + 5 (data)
    }

    @Test
    public void testMultiplePositionChanges() throws IOException {
        ByteArrayDataOutputStream baos = new ByteArrayDataOutputStream(64);
        RandomAccessDataOutputStream adapter = new RandomAccessDataOutputStream(baos);

        // Write sequence: int, int, int
        adapter.writeInt(111);
        adapter.writeInt(222);
        adapter.writeInt(333);

        // Jump around and patch values
        adapter.position(0);
        adapter.writeInt(999);

        adapter.position(8);
        adapter.writeInt(888);

        adapter.position(4);
        adapter.writeInt(777);

        // Read back
        byte[] buffer = baos.buffer();

        int val0 = ((buffer[0] & 0xFF) << 24) | ((buffer[1] & 0xFF) << 16) |
                   ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF);
        int val1 = ((buffer[4] & 0xFF) << 24) | ((buffer[5] & 0xFF) << 16) |
                   ((buffer[6] & 0xFF) << 8) | (buffer[7] & 0xFF);
        int val2 = ((buffer[8] & 0xFF) << 24) | ((buffer[9] & 0xFF) << 16) |
                   ((buffer[10] & 0xFF) << 8) | (buffer[11] & 0xFF);

        assertThat(val0).isEqualTo(999);
        assertThat(val1).isEqualTo(777);
        assertThat(val2).isEqualTo(888);
    }
}
