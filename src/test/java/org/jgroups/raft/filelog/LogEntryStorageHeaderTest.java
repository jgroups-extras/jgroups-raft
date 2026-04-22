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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LogEntryStorageHeaderTest {

    private static final byte[] MAGIC_BYTES = {'R', 'A', 'F', 'T'};
    private static final int FILE_HEADER_SIZE = 8;
    private static final byte CURRENT_VERSION = 2;
    private static final byte LEGACY_ENTRY_MAGIC = 0x01;
    private static final byte CRC_ENTRY_MAGIC = 0x02;

    private Path tempDir;
    private LogEntryStorage storage;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log-entry-storage-header-test");
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

    public void testNewEmptyFileGetsHeader() throws IOException {
        storage = createStorage();
        storage.open();

        byte[] rawHeader = readRawBytes(0, FILE_HEADER_SIZE);
        assertThat(rawHeader).hasSize(FILE_HEADER_SIZE);
        assertThat(rawHeader[0]).isEqualTo(MAGIC_BYTES[0]);
        assertThat(rawHeader[1]).isEqualTo(MAGIC_BYTES[1]);
        assertThat(rawHeader[2]).isEqualTo(MAGIC_BYTES[2]);
        assertThat(rawHeader[3]).isEqualTo(MAGIC_BYTES[3]);
        assertThat(rawHeader[4]).isEqualTo(CURRENT_VERSION);
        assertThat(rawHeader[5]).isZero();
        assertThat(rawHeader[6]).isZero();
        assertThat(rawHeader[7]).isZero();
    }

    public void testNewFileEntriesStartAfterHeader() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "first"), entry(1, "second"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(2);
        LogEntry first = storage.getLogEntry(1);
        assertThat(first).isNotNull();
        assertThat(first.term()).isEqualTo(1);
        assertThat(new String(first.command())).isEqualTo("first");

        LogEntry second = storage.getLogEntry(2);
        assertThat(second).isNotNull();
        assertThat(second.term()).isEqualTo(1);
        assertThat(new String(second.command())).isEqualTo("second");
    }

    public void testNewFileFirstEntryAtOffsetAfterHeader() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "data"));

        byte[] firstByte = readRawBytes(FILE_HEADER_SIZE, 1);
        assertThat(firstByte[0]).isEqualTo(CRC_ENTRY_MAGIC);
    }

    public void testLegacyFileOpensInCompatibilityMode() throws IOException {
        writeLegacyFile(entry(1, "legacy-one"), entry(1, "legacy-two"), entry(2, "legacy-three"));

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(3);

        LogEntry first = storage.getLogEntry(1);
        assertThat(first).isNotNull();
        assertThat(first.term()).isEqualTo(1);
        assertThat(new String(first.command())).isEqualTo("legacy-one");

        LogEntry third = storage.getLogEntry(3);
        assertThat(third).isNotNull();
        assertThat(third.term()).isEqualTo(2);
        assertThat(new String(third.command())).isEqualTo("legacy-three");
    }

    public void testLegacyFileAppendPreservesExistingEntries() throws IOException {
        writeLegacyFile(entry(1, "existing"));

        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(2, entry(1, "appended"));

        assertThat(storage.getLastAppended()).isEqualTo(2);
        LogEntry existing = storage.getLogEntry(1);
        assertThat(existing).isNotNull();
        assertThat(new String(existing.command())).isEqualTo("existing");

        LogEntry appended = storage.getLogEntry(2);
        assertThat(appended).isNotNull();
        assertThat(new String(appended.command())).isEqualTo("appended");
    }

    public void testUnknownVersionRefusesToOpen() throws IOException {
        writeFileHeader((byte) 99);

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    public void testVersionZeroRefusesToOpen() throws IOException {
        writeFileHeader((byte) 0);

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    public void testReinitializeToWritesHeader() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        storage.reinitializeTo(10, entry(3, "snapshot-entry"));

        byte[] rawHeader = readRawBytes(0, FILE_HEADER_SIZE);
        assertThat(rawHeader[0]).isEqualTo(MAGIC_BYTES[0]);
        assertThat(rawHeader[1]).isEqualTo(MAGIC_BYTES[1]);
        assertThat(rawHeader[2]).isEqualTo(MAGIC_BYTES[2]);
        assertThat(rawHeader[3]).isEqualTo(MAGIC_BYTES[3]);
        assertThat(rawHeader[4]).isEqualTo(CURRENT_VERSION);
    }

    public void testReinitializeToPreservesEntryData() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "will-be-replaced"));

        storage.reinitializeTo(10, entry(3, "snapshot-entry"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(10);
        assertThat(storage.getFirstAppended()).isEqualTo(10);

        LogEntry loaded = storage.getLogEntry(10);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(3);
        assertThat(new String(loaded.command())).isEqualTo("snapshot-entry");

        assertThat(storage.getLogEntry(1)).isNull();
    }

    public void testReinitializeToOnLegacyFileProducesHeaderedFile() throws IOException {
        writeLegacyFile(entry(1, "old"));

        storage = createStorage();
        storage.open();
        storage.reload();

        storage.reinitializeTo(5, entry(2, "new-snapshot"));
        storage.close();

        byte[] rawHeader = readRawBytes(0, FILE_HEADER_SIZE);
        assertThat(rawHeader[0]).isEqualTo(MAGIC_BYTES[0]);
        assertThat(rawHeader[1]).isEqualTo(MAGIC_BYTES[1]);
        assertThat(rawHeader[2]).isEqualTo(MAGIC_BYTES[2]);
        assertThat(rawHeader[3]).isEqualTo(MAGIC_BYTES[3]);

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(5);
        LogEntry loaded = storage.getLogEntry(5);
        assertThat(loaded).isNotNull();
        assertThat(new String(loaded.command())).isEqualTo("new-snapshot");
    }

    public void testCorruptedFirstBytesRefusesToOpen() throws IOException {
        writeRawBytes(new byte[]{0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class);
    }

    public void testForEachWorksAfterReloadWithHeader() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "a"), entry(1, "b"), entry(2, "c"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        List<String> collected = new ArrayList<>();
        storage.forEach((e, idx) -> collected.add(new String(e.command())), 1, 3);
        assertThat(collected).containsExactly("a", "b", "c");
    }

    public void testRemoveOldWorksWithHeader() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "a"), entry(1, "b"), entry(1, "c"));

        storage.removeOld(2);
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(2);
        assertThat(storage.getLastAppended()).isEqualTo(3);
        assertThat(storage.getLogEntry(1)).isNull();
        assertThat(storage.getLogEntry(2)).isNotNull();
    }

    public void testRemoveNewWorksWithHeader() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "a"), entry(1, "b"), entry(1, "c"));

        storage.removeNew(3);
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(2);
        assertThat(storage.getLogEntry(3)).isNull();
        assertThat(storage.getLogEntry(2)).isNotNull();
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

    private void writeLegacyFile(LogEntry... entries) throws IOException {
        File file = tempDir.resolve("entries.raft").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long position = 0;
            long index = 1;
            for (LogEntry entry : entries) {
                int dataLength = entry.length();
                int headerSize = 1 + 4 + 8 + 8 + 1 + 4;
                int totalLength = headerSize + dataLength;

                ByteBuffer buffer = ByteBuffer.allocate(totalLength);
                buffer.put(LEGACY_ENTRY_MAGIC);
                buffer.putInt(totalLength);
                buffer.putLong(entry.term());
                buffer.putLong(index);
                buffer.put(entry.internal() ? (byte) 1 : (byte) 0);
                buffer.putInt(dataLength);
                if (dataLength > 0) {
                    buffer.put(entry.command(), entry.offset(), dataLength);
                }
                buffer.flip();

                raf.getChannel().write(buffer, position);
                position += totalLength;
                index++;
            }
        }
    }

    private void writeFileHeader(byte version) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_SIZE);
        header.put(MAGIC_BYTES);
        header.put(version);
        header.put((byte) 0);
        header.put((byte) 0);
        header.put((byte) 0);
        header.flip();

        File file = tempDir.resolve("entries.raft").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.getChannel().write(header, 0);
        }
    }

    private void writeRawBytes(byte[] bytes) throws IOException {
        File file = tempDir.resolve("entries.raft").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.write(bytes);
        }
    }

    private byte[] readRawBytes(long position, int length) throws IOException {
        File file = tempDir.resolve("entries.raft").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            ByteBuffer buffer = ByteBuffer.allocate(length);
            raf.getChannel().read(buffer, position);
            buffer.flip();
            byte[] result = new byte[buffer.remaining()];
            buffer.get(result);
            return result;
        }
    }
}
