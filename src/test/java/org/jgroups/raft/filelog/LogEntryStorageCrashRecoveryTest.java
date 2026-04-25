package org.jgroups.raft.filelog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LogEntryStorageCrashRecoveryTest {

    private static final byte LEGACY_MAGIC = 0x01;
    private static final int FILE_HEADER_SIZE = 8;
    private static final int ENTRY_HEADER_SIZE = 26;
    private static final int CRC_SIZE = 4;

    private Path tempDir;
    private LogEntryStorage storage;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log-crash-recovery-test");
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (storage != null) {
            storage.close();
        }
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testTruncatedEntryDataThrowsOnRead() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "first"), entry(1, "second"), entry(2, "third-with-longer-data"));
        storage.close();

        int entry1Size = ENTRY_HEADER_SIZE + "first".length() + CRC_SIZE;
        int entry2Size = ENTRY_HEADER_SIZE + "second".length() + CRC_SIZE;
        int entry3Size = ENTRY_HEADER_SIZE + "third-with-longer-data".length() + CRC_SIZE;
        long dataEnd = FILE_HEADER_SIZE + entry1Size + entry2Size + entry3Size;
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.setLength(dataEnd - 5);
        }

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLogEntry(1)).isNotNull();
        assertThat(storage.getLogEntry(2)).isNotNull();
        assertThatThrownBy(() -> storage.getLogEntry(3))
                .isInstanceOf(IOException.class);
    }

    public void testTruncatedEntryHeaderStopsReloadGracefully() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "alpha"), entry(1, "beta"));
        storage.close();

        long secondEntryPos = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE + "alpha".length() + CRC_SIZE;
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.setLength(secondEntryPos + 10);
        }

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(1);
        assertThat(storage.getLogEntry(1)).isNotNull();
    }

    public void testPositionCacheEntryWithNullHeaderThrows() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "data"));

        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(FILE_HEADER_SIZE);
            raf.write(new byte[ENTRY_HEADER_SIZE]);
        }

        assertThatThrownBy(() -> storage.getLogEntry(1))
                .isInstanceOf(IOException.class);
    }

    public void testFileNotModifiedAfterCorruptionDetectedOnReload() throws Exception {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "keep-intact"));
        storage.close();

        long termPosition = FILE_HEADER_SIZE + 1 + 4;
        corruptByteAt(termPosition);

        byte[] digestBefore = fileDigest(entriesPath());

        storage = createStorage();
        storage.open();

        try {
            storage.reload();
        } catch (IOException ignored) {
        }

        byte[] digestAfter = fileDigest(entriesPath());
        assertThat(digestAfter).isEqualTo(digestBefore);
    }

    public void testFileNotModifiedAfterCorruptionDetectedOnRead() throws Exception {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "valid-data"));
        storage.close();

        long dataPosition = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE;
        corruptByteAt(dataPosition);

        byte[] digestBefore = fileDigest(entriesPath());

        storage = createStorage();
        storage.open();
        storage.reload();

        try {
            storage.getLogEntry(1);
        } catch (IOException ignored) {
        }

        byte[] digestAfter = fileDigest(entriesPath());
        assertThat(digestAfter).isEqualTo(digestBefore);
    }

    public void testLegacyEntryWithZeroTermThrowsOnReload() throws IOException {
        writeLegacyEntry(0, 1, "bad-term");

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class);
    }

    public void testLegacyEntryWithNegativeDataLengthThrowsOnReload() throws IOException {
        writeLegacyEntryRaw(LEGACY_MAGIC, 1, 1, -1, new byte[0]);

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class);
    }

    public void testCorruptedMiddleEntryDetectedOnRead() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "aaa"), entry(1, "bbb"), entry(1, "ccc"), entry(2, "ddd"), entry(2, "eee"));
        storage.close();

        long entry3Pos = FILE_HEADER_SIZE;
        for (int i = 0; i < 2; i++) {
            entry3Pos += ENTRY_HEADER_SIZE + 3 + CRC_SIZE;
        }
        corruptByteAt(entry3Pos + ENTRY_HEADER_SIZE);

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(new String(storage.getLogEntry(1).command())).isEqualTo("aaa");
        assertThat(new String(storage.getLogEntry(2).command())).isEqualTo("bbb");
        assertThatThrownBy(() -> storage.getLogEntry(3))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");
        assertThat(new String(storage.getLogEntry(4).command())).isEqualTo("ddd");
        assertThat(new String(storage.getLogEntry(5).command())).isEqualTo("eee");
    }

    public void testErrorMessageContainsPositionAndIndex() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "test-data"));
        storage.close();

        long dataPosition = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE;
        corruptByteAt(dataPosition);

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThatThrownBy(() -> storage.getLogEntry(1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("index 1")
                .hasMessageContaining("position")
                .hasMessageContaining("raft log verify");
    }

    public void testErrorMessageOnHeaderCorruptionContainsPosition() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "data"));
        storage.close();

        long termPosition = FILE_HEADER_SIZE + 1 + 4;
        corruptByteAt(termPosition);

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("position")
                .hasMessageContaining("raft log verify");
    }

    private LogEntryStorage createStorage() {
        return new LogEntryStorage(tempDir.toFile(), false);
    }

    private LogEntry entry(int term, String data) {
        return new LogEntry(term, data.getBytes());
    }

    private void writeEntries(long startIndex, LogEntry... entries) throws IOException {
        LogEntries le = new LogEntries();
        for (LogEntry entry : entries) {
            le.add(entry);
        }
        storage.write(startIndex, le);
    }

    private Path entriesPath() {
        return tempDir.resolve("entries.raft");
    }

    private void corruptByteAt(long position) throws IOException {
        File file = entriesPath().toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(position);
            byte original = raf.readByte();
            raf.seek(position);
            raf.writeByte(original ^ 0xFF);
        }
    }

    private void writeLegacyEntry(long term, long index, String data) throws IOException {
        byte[] dataBytes = data.getBytes();
        writeLegacyEntryRaw(LEGACY_MAGIC, term, index, dataBytes.length, dataBytes);
    }

    private void writeLegacyEntryRaw(byte magic, long term, long index, int dataLength, byte[] data) throws IOException {
        File file = entriesPath().toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            int totalLength = ENTRY_HEADER_SIZE + Math.max(dataLength, 0);

            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_HEADER_SIZE + data.length);
            buffer.put(magic);
            buffer.putInt(totalLength);
            buffer.putLong(term);
            buffer.putLong(index);
            buffer.put((byte) 0);
            buffer.putInt(dataLength);
            if (data.length > 0) {
                buffer.put(data);
            }
            buffer.flip();

            raf.getChannel().write(buffer, 0);
        }
    }

    private static byte[] fileDigest(Path path) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(Files.readAllBytes(path));
        return md.digest();
    }
}
