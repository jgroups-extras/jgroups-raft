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
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LogEntryStorageCrcTest {

    private static final byte LEGACY_MAGIC = 0x01;
    private static final byte CRC_MAGIC = 0x02;
    private static final int FILE_HEADER_SIZE = 8;
    private static final int ENTRY_HEADER_SIZE = 26;
    private static final int CRC_SIZE = 4;

    private Path tempDir;
    private LogEntryStorage storage;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log-entry-storage-crc-test");
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

    public void testNewEntryWrittenWithCrcMagic() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "hello"));

        byte[] magic = readRawBytes(FILE_HEADER_SIZE, 1);
        assertThat(magic[0]).isEqualTo(CRC_MAGIC);

        int dataLength = 5;
        long crcPosition = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE + dataLength;
        byte[] storedCrc = readRawBytes(crcPosition, CRC_SIZE);
        assertThat(storedCrc).hasSize(CRC_SIZE);

        byte[] headerAndData = readRawBytes(FILE_HEADER_SIZE, ENTRY_HEADER_SIZE + dataLength);
        int expectedCrc = computeCrc32c(headerAndData);
        int actualCrc = ByteBuffer.wrap(storedCrc).getInt();
        assertThat(actualCrc).isEqualTo(expectedCrc);
    }

    public void testCrcEntryRoundtrip() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(3, "roundtrip-data"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(1);
        LogEntry loaded = storage.getLogEntry(1);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(3);
        assertThat(new String(loaded.command())).isEqualTo("roundtrip-data");
    }

    public void testBatchCrcEntryRoundtrip() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "alpha"), entry(2, "beta"), entry(3, "gamma"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(3);

        LogEntry first = storage.getLogEntry(1);
        assertThat(first).isNotNull();
        assertThat(first.term()).isEqualTo(1);
        assertThat(new String(first.command())).isEqualTo("alpha");

        LogEntry second = storage.getLogEntry(2);
        assertThat(second).isNotNull();
        assertThat(second.term()).isEqualTo(2);
        assertThat(new String(second.command())).isEqualTo("beta");

        LogEntry third = storage.getLogEntry(3);
        assertThat(third).isNotNull();
        assertThat(third.term()).isEqualTo(3);
        assertThat(new String(third.command())).isEqualTo("gamma");
    }

    public void testCorruptedDataDetectedByCrc() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "intact"));
        storage.close();

        long dataPosition = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE;
        corruptByteAt(dataPosition);

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");

        assertThatThrownBy(() -> storage.getLogEntry(1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");
    }

    public void testCorruptedCrcBytesDetected() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "valid"));
        storage.close();

        int dataLength = "valid".length();
        long crcPosition = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE + dataLength;
        corruptByteAt(crcPosition);

        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.reload())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");

        assertThatThrownBy(() -> storage.getLogEntry(1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");
    }

    public void testCorruptedHeaderDetectedByCrc() throws IOException {
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
                .isInstanceOf(IOException.class);
    }

    public void testMixedLegacyAndCrcEntries() throws IOException {
        writeLegacyEntries(entry(1, "legacy-one"), entry(1, "legacy-two"));

        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(3, entry(2, "new-three"), entry(2, "new-four"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(4);

        LogEntry legacyFirst = storage.getLogEntry(1);
        assertThat(legacyFirst).isNotNull();
        assertThat(legacyFirst.term()).isEqualTo(1);
        assertThat(new String(legacyFirst.command())).isEqualTo("legacy-one");

        LogEntry legacySecond = storage.getLogEntry(2);
        assertThat(legacySecond).isNotNull();
        assertThat(new String(legacySecond.command())).isEqualTo("legacy-two");

        LogEntry newThird = storage.getLogEntry(3);
        assertThat(newThird).isNotNull();
        assertThat(newThird.term()).isEqualTo(2);
        assertThat(new String(newThird.command())).isEqualTo("new-three");

        LogEntry newFourth = storage.getLogEntry(4);
        assertThat(newFourth).isNotNull();
        assertThat(new String(newFourth.command())).isEqualTo("new-four");
    }

    public void testPositionCacheCorrectWithMixedEntries() throws IOException {
        writeLegacyEntries(entry(1, "short"), entry(1, "a-longer-legacy-entry"));

        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(3, entry(2, "x"), entry(2, "another-crc-entry-with-more-data"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(new String(storage.getLogEntry(1).command())).isEqualTo("short");
        assertThat(new String(storage.getLogEntry(2).command())).isEqualTo("a-longer-legacy-entry");
        assertThat(new String(storage.getLogEntry(3).command())).isEqualTo("x");
        assertThat(new String(storage.getLogEntry(4).command())).isEqualTo("another-crc-entry-with-more-data");
    }

    public void testZeroLengthEntryWithCrc() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, new LogEntry(5, null));
        storage.close();

        byte[] magic = readRawBytes(FILE_HEADER_SIZE, 1);
        assertThat(magic[0]).isEqualTo(CRC_MAGIC);

        long crcPosition = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE;
        byte[] storedCrc = readRawBytes(crcPosition, CRC_SIZE);
        assertThat(ByteBuffer.wrap(storedCrc).getInt()).isNotZero();

        storage = createStorage();
        storage.open();
        storage.reload();

        LogEntry loaded = storage.getLogEntry(1);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(5);
        assertThat(loaded.length()).isZero();
    }

    public void testReinitializeToWritesCrcEntry() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "will-be-replaced"));
        storage.reinitializeTo(10, entry(4, "snapshot"));
        storage.close();

        byte[] magic = readRawBytes(FILE_HEADER_SIZE, 1);
        assertThat(magic[0]).isEqualTo(CRC_MAGIC);

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(10);
        LogEntry loaded = storage.getLogEntry(10);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(4);
        assertThat(new String(loaded.command())).isEqualTo("snapshot");
    }

    public void testReinitializeToWithNullCommandWritesCrc() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "will-be-replaced"));

        storage.reinitializeTo(100, new LogEntry(5, null));
        storage.close();

        byte[] magic = readRawBytes(FILE_HEADER_SIZE, 1);
        assertThat(magic[0]).isEqualTo(CRC_MAGIC);

        long crcPosition = FILE_HEADER_SIZE + ENTRY_HEADER_SIZE;
        byte[] storedCrc = readRawBytes(crcPosition, CRC_SIZE);
        assertThat(ByteBuffer.wrap(storedCrc).getInt()).isNotZero();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(100);
        assertThat(storage.getLastAppended()).isEqualTo(100);

        LogEntry loaded = storage.getLogEntry(100);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(5);
        assertThat(loaded.length()).isZero();
    }

    public void testReinitializeToWithNullCommandThenAppendSurvivesRestart() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        storage.reinitializeTo(50, new LogEntry(3, null));

        writeEntries(51, entry(3, "after-snap-1"), entry(4, "after-snap-2"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(50);
        assertThat(storage.getLastAppended()).isEqualTo(52);

        LogEntry snap = storage.getLogEntry(50);
        assertThat(snap).isNotNull();
        assertThat(snap.term()).isEqualTo(3);
        assertThat(snap.length()).isZero();

        assertThat(new String(storage.getLogEntry(51).command())).isEqualTo("after-snap-1");
        assertThat(new String(storage.getLogEntry(52).command())).isEqualTo("after-snap-2");
    }

    public void testRepeatedReinitializeToWithNullCommand() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        storage.reinitializeTo(100, new LogEntry(3, null));
        writeEntries(101, entry(3, "after-first-snap"));

        storage.reinitializeTo(200, new LogEntry(5, null));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(200);
        assertThat(storage.getLastAppended()).isEqualTo(200);

        LogEntry loaded = storage.getLogEntry(200);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(5);
        assertThat(loaded.length()).isZero();

        assertThat(storage.getLogEntry(100)).isNull();
        assertThat(storage.getLogEntry(101)).isNull();
    }

    public void testForEachOverNullCommandEntry() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        storage.reinitializeTo(10, new LogEntry(2, null));
        writeEntries(11, entry(2, "real-data"), entry(3, "more-data"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        int[] count = {0};
        storage.forEach((entry, index) -> {
            if (index == 10) {
                assertThat(entry.term()).isEqualTo(2);
                assertThat(entry.length()).isZero();
            } else {
                assertThat(entry.command()).isNotNull();
            }
            count[0]++;
        }, 10, 12);
        assertThat(count[0]).isEqualTo(3);
    }

    public void testReinitializeToOnLegacyUpgradesToCrc() throws IOException {
        writeLegacyEntries(entry(1, "old-one"), entry(1, "old-two"));

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(2);

        storage.reinitializeTo(5, entry(3, "upgraded-snapshot"));
        storage.close();

        byte[] fileHeader = readRawBytes(0, 4);
        assertThat(fileHeader[0]).isEqualTo((byte) 'R');

        byte[] entryMagic = readRawBytes(FILE_HEADER_SIZE, 1);
        assertThat(entryMagic[0]).isEqualTo(CRC_MAGIC);

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(5);
        assertThat(storage.getLastAppended()).isEqualTo(5);

        LogEntry loaded = storage.getLogEntry(5);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(3);
        assertThat(new String(loaded.command())).isEqualTo("upgraded-snapshot");

        assertThat(storage.getLogEntry(1)).isNull();
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

    private void writeLegacyEntries(LogEntry... entries) throws IOException {
        File file = tempDir.resolve("entries.raft").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long position = 0;
            long index = 1;
            for (LogEntry entry : entries) {
                int dataLength = entry.length();
                int totalLength = ENTRY_HEADER_SIZE + dataLength;

                ByteBuffer buffer = ByteBuffer.allocate(totalLength);
                buffer.put(LEGACY_MAGIC);
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

    private void corruptByteAt(long position) throws IOException {
        File file = tempDir.resolve("entries.raft").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(position);
            byte original = raf.readByte();
            raf.seek(position);
            raf.writeByte(original ^ 0xFF);
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

    private static int computeCrc32c(byte[] data) {
        CRC32C crc = new CRC32C();
        crc.update(data);
        return (int) crc.getValue();
    }
}
