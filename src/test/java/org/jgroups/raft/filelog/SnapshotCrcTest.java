package org.jgroups.raft.filelog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SnapshotCrcTest {

    private static final byte[] SNAP_MAGIC = {'S', 'N', 'A', 'P'};
    private static final int SNAP_HEADER_SIZE = 8;
    private static final int CRC_SIZE = 4;
    private static final String SNAPSHOT_FILE = "state_snapshot.raft";

    private Path tempDir;
    private SnapshotStorage storage;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("snapshot-crc-test");
        storage = new SnapshotStorage(tempDir.toFile());
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testNewSnapshotWrittenWithHeader() throws IOException {
        byte[] data = "snapshot-data".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        byte[] rawHeader = readRawBytes(0, SNAP_HEADER_SIZE);
        assertThat(rawHeader[0]).isEqualTo(SNAP_MAGIC[0]);
        assertThat(rawHeader[1]).isEqualTo(SNAP_MAGIC[1]);
        assertThat(rawHeader[2]).isEqualTo(SNAP_MAGIC[2]);
        assertThat(rawHeader[3]).isEqualTo(SNAP_MAGIC[3]);
        assertThat(rawHeader[4]).isEqualTo((byte) 2);
        assertThat(rawHeader[5]).isZero();
        assertThat(rawHeader[6]).isZero();
        assertThat(rawHeader[7]).isZero();

        long fileSize = Files.size(snapshotPath());
        assertThat(fileSize).isEqualTo(SNAP_HEADER_SIZE + data.length + CRC_SIZE);

        byte[] storedCrc = readRawBytes(fileSize - CRC_SIZE, CRC_SIZE);
        byte[] snapshotData = readRawBytes(SNAP_HEADER_SIZE, data.length);
        int expectedCrc = computeCrc32c(snapshotData);
        int actualCrc = ByteBuffer.wrap(storedCrc).getInt();
        assertThat(actualCrc).isEqualTo(expectedCrc);
    }

    public void testSnapshotRoundtrip() throws IOException {
        byte[] data = "roundtrip-snapshot".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        ByteBuffer loaded = storage.readSnapshot();
        assertThat(loaded).isNotNull();
        byte[] result = new byte[loaded.remaining()];
        loaded.get(result);
        assertThat(result).isEqualTo(data);
    }

    public void testLegacySnapshotReadWithoutHeader() throws IOException {
        byte[] data = "legacy-snapshot-data".getBytes();
        Files.write(snapshotPath(), data);

        ByteBuffer loaded = storage.readSnapshot();
        assertThat(loaded).isNotNull();
        byte[] result = new byte[loaded.remaining()];
        loaded.get(result);
        assertThat(result).isEqualTo(data);
    }

    public void testCorruptedSnapshotDataDetectedByCrc() throws IOException {
        byte[] data = "will-be-corrupted".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        corruptByteAt(SNAP_HEADER_SIZE);

        assertThatThrownBy(() -> storage.readSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");
    }

    public void testCorruptedSnapshotCrcDetected() throws IOException {
        byte[] data = "valid-data".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        long fileSize = Files.size(snapshotPath());
        corruptByteAt(fileSize - CRC_SIZE);

        assertThatThrownBy(() -> storage.readSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");
    }

    public void testUnknownSnapshotVersionRefusesToRead() throws IOException {
        writeRawSnapshot((byte) 99, "some-data".getBytes());

        assertThatThrownBy(() -> storage.readSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    public void testVersionZeroRefusesToRead() throws IOException {
        writeRawSnapshot((byte) 0, "some-data".getBytes());

        assertThatThrownBy(() -> storage.readSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    public void testEmptySnapshotRoundtrip() throws IOException {
        storage.writeSnapshot(new ByteArrayInputStream(new byte[0]));

        long fileSize = Files.size(snapshotPath());
        assertThat(fileSize).isEqualTo(SNAP_HEADER_SIZE + CRC_SIZE);

        ByteBuffer loaded = storage.readSnapshot();
        assertThat(loaded).isNotNull();
        assertThat(loaded.remaining()).isZero();
    }

    public void testSnapshotOverwritePreservesNewData() throws IOException {
        storage.writeSnapshot(new ByteArrayInputStream("first-snapshot".getBytes()));
        storage.writeSnapshot(new ByteArrayInputStream("second-snapshot".getBytes()));

        ByteBuffer loaded = storage.readSnapshot();
        assertThat(loaded).isNotNull();
        byte[] result = new byte[loaded.remaining()];
        loaded.get(result);
        assertThat(new String(result)).isEqualTo("second-snapshot");
    }

    public void testNoSnapshotReturnsNull() throws IOException {
        assertThat(storage.readSnapshot()).isNull();
    }

    public void testRepeatedWriteAndReadCycles() throws IOException {
        String[] snapshots = {"first", "second-longer-data", "x", "fourth-snapshot-with-more-content", ""};
        for (String data : snapshots) {
            storage.writeSnapshot(new ByteArrayInputStream(data.getBytes()));

            ByteBuffer loaded = storage.readSnapshot();
            assertThat(loaded).isNotNull();
            byte[] result = new byte[loaded.remaining()];
            loaded.get(result);
            assertThat(new String(result)).isEqualTo(data);
        }
    }

    public void testTruncatedSnapshotFileDetected() throws IOException {
        byte[] data = "truncated".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        try (RandomAccessFile raf = new RandomAccessFile(snapshotPath().toFile(), "rw")) {
            raf.setLength(SNAP_HEADER_SIZE + 2);
        }

        assertThatThrownBy(() -> storage.readSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("truncated");
    }

    public void testHeaderOnlySnapshotFileDetected() throws IOException {
        writeRawSnapshot((byte) 2, new byte[0]);

        assertThatThrownBy(() -> storage.readSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("truncated");
    }

    public void testSnapshotSizeV2() throws IOException {
        byte[] data = "snapshot-size-test".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        assertThat(storage.snapshotSize()).isEqualTo(data.length);
    }

    public void testSnapshotSizeNoFile() throws IOException {
        assertThat(storage.snapshotSize()).isZero();
    }

    public void testSnapshotSizeLegacy() throws IOException {
        byte[] data = "legacy-size".getBytes();
        Files.write(snapshotPath(), data);

        assertThat(storage.snapshotSize()).isEqualTo(data.length);
    }

    public void testSnapshotSizeEmpty() throws IOException {
        storage.writeSnapshot(new ByteArrayInputStream(new byte[0]));

        assertThat(storage.snapshotSize()).isZero();
    }

    public void testSnapshotSizeAfterOverwrite() throws IOException {
        storage.writeSnapshot(new ByteArrayInputStream("short".getBytes()));
        storage.writeSnapshot(new ByteArrayInputStream("much-longer-snapshot-data".getBytes()));

        assertThat(storage.snapshotSize()).isEqualTo("much-longer-snapshot-data".length());
    }

    public void testRegionReadsFullData() throws IOException {
        byte[] data = "region-full-read".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        byte[] dst = new byte[data.length];
        int read = storage.region(0, dst, 0, data.length);

        assertThat(read).isEqualTo(data.length);
        assertThat(dst).isEqualTo(data);
    }

    public void testRegionReadsPartialChunk() throws IOException {
        byte[] data = "abcdefghijklmnopqrstuvwxyz".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        byte[] dst = new byte[10];
        int read = storage.region(5, dst, 0, 10);

        assertThat(read).isEqualTo(10);
        assertThat(dst).isEqualTo("fghijklmno".getBytes());
    }

    public void testRegionReadsLastBytes() throws IOException {
        byte[] data = "abcdefghijklmnop".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        byte[] dst = new byte[6];
        int read = storage.region(10, dst, 0, 6);

        assertThat(read).isEqualTo(6);
        assertThat(dst).isEqualTo("klmnop".getBytes());
    }

    public void testRegionWithDstOffset() throws IOException {
        byte[] data = "offset-test-data".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        byte[] dst = new byte[20];
        int read = storage.region(0, dst, 5, 10);

        assertThat(read).isEqualTo(10);
        byte[] slice = new byte[10];
        System.arraycopy(dst, 5, slice, 0, 10);
        assertThat(slice).isEqualTo("offset-tes".getBytes());
    }

    public void testRegionNoFile() throws IOException {
        byte[] dst = new byte[10];
        int read = storage.region(0, dst, 0, 10);

        assertThat(read).isZero();
    }

    public void testRegionLegacySnapshot() throws IOException {
        byte[] data = "legacy-region".getBytes();
        Files.write(snapshotPath(), data);

        byte[] dst = new byte[data.length];
        int read = storage.region(0, dst, 0, data.length);

        assertThat(read).isEqualTo(data.length);
        assertThat(dst).isEqualTo(data);
    }

    public void testRegionMultipleReadsReassemble() throws IOException {
        byte[] data = "reassemble-this-snapshot-data".getBytes();
        storage.writeSnapshot(new ByteArrayInputStream(data));

        int chunkSize = 8;
        byte[] reassembled = new byte[data.length];
        int totalChunks = (int) Math.ceil((double) data.length / chunkSize);

        for (int i = 0; i < totalChunks; i++) {
            int offset = i * chunkSize;
            int len = Math.min(chunkSize, data.length - offset);
            int read = storage.region(offset, reassembled, offset, len);
            assertThat(read).isEqualTo(len);
        }

        assertThat(reassembled).isEqualTo(data);
    }

    public void testLegacySnapshotOverwrittenWithNewFormat() throws IOException {
        Files.write(snapshotPath(), "legacy-data".getBytes());

        storage.writeSnapshot(new ByteArrayInputStream("new-data".getBytes()));

        byte[] rawHeader = readRawBytes(0, SNAP_HEADER_SIZE);
        assertThat(rawHeader[0]).isEqualTo(SNAP_MAGIC[0]);
        assertThat(rawHeader[1]).isEqualTo(SNAP_MAGIC[1]);
        assertThat(rawHeader[2]).isEqualTo(SNAP_MAGIC[2]);
        assertThat(rawHeader[3]).isEqualTo(SNAP_MAGIC[3]);

        ByteBuffer loaded = storage.readSnapshot();
        assertThat(loaded).isNotNull();
        byte[] result = new byte[loaded.remaining()];
        loaded.get(result);
        assertThat(new String(result)).isEqualTo("new-data");
    }

    private Path snapshotPath() {
        return tempDir.resolve(SNAPSHOT_FILE);
    }

    private void corruptByteAt(long position) throws IOException {
        File file = snapshotPath().toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(position);
            byte original = raf.readByte();
            raf.seek(position);
            raf.writeByte(original ^ 0xFF);
        }
    }

    private byte[] readRawBytes(long position, int length) throws IOException {
        File file = snapshotPath().toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            ByteBuffer buffer = ByteBuffer.allocate(length);
            raf.getChannel().read(buffer, position);
            buffer.flip();
            byte[] result = new byte[buffer.remaining()];
            buffer.get(result);
            return result;
        }
    }

    private void writeRawSnapshot(byte version, byte[] data) throws IOException {
        File file = snapshotPath().toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            ByteBuffer header = ByteBuffer.allocate(SNAP_HEADER_SIZE + data.length);
            header.put(SNAP_MAGIC);
            header.put(version);
            header.put((byte) 0);
            header.put((byte) 0);
            header.put((byte) 0);
            header.put(data);
            header.flip();
            raf.getChannel().write(header, 0);
        }
    }

    private static int computeCrc32c(byte[] data) {
        CRC32C crc = new CRC32C();
        crc.update(data);
        return (int) crc.getValue();
    }
}
