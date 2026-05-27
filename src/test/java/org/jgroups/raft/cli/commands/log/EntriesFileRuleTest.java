package org.jgroups.raft.cli.commands.log;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.raft.filelog.LogEntryStorage;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class EntriesFileRuleTest {

    private static final byte[] RAFT_MAGIC = LogEntryStorage.FILE_HEADER_MAGIC;
    private static final int FILE_HEADER_SIZE = LogEntryStorage.FILE_HEADER_SIZE;
    private static final byte V2_ENTRY_MAGIC = LogEntryStorage.MAGIC_NUMBER_CRC;
    private static final byte V1_ENTRY_MAGIC = LogEntryStorage.MAGIC_NUMBER;
    private static final int ENTRY_HEADER_SIZE = LogEntryStorage.HEADER_SIZE;
    private static final int CRC_SIZE = LogEntryStorage.CRC_SIZE;
    private static final String ENTRIES_FILE = LogEntryStorage.FILE_NAME;

    private Path tempDir;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("entries-file-rule-test");
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

    public void testValidHeaderNoEntries() throws Exception {
        // just create and close
        FileBasedLog fbl = createLog();
        fbl.close();

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
    }

    public void testValidEntries() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, "first".getBytes()),
                    new LogEntry(2, "second".getBytes()),
                    new LogEntry(3, "third".getBytes())
            ));
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(result.firstLogIndex()).hasValue(1);
        assertThat(result.lastLogIndex()).hasValue(3);
        assertThat(result.highestTerm()).hasValue(3);
        String output = formatOutput(result);
        assertThat(output).contains("1 - 3");
        assertThat(output).contains("3 entries");
    }

    public void testHighestTermTrackedAcrossEntries() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(3, "a".getBytes()),
                    new LogEntry(7, "b".getBytes()),
                    new LogEntry(5, "c".getBytes())
            ));
        }

        ValidationContext result = validate();

        assertThat(result.highestTerm()).hasValue(7);
    }

    public void testZeroLengthDataEntry() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, null),
                    new LogEntry(2, "notempty".getBytes())
            ));
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(result.firstLogIndex()).hasValue(1);
        assertThat(result.lastLogIndex()).hasValue(2);
    }

    public void testEmptyEntriesFile() throws IOException {
        Files.createFile(tempDir.resolve(ENTRIES_FILE));

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        assertThat(formatOutput(result)).containsIgnoringCase("EMPTY");
        assertThat(result.firstLogIndex()).isEmpty();
        assertThat(result.lastLogIndex()).isEmpty();
        assertThat(result.highestTerm()).isEmpty();
    }

    public void testMissingEntriesFile() throws IOException {
        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        assertThat(formatOutput(result)).contains("entries.raft");
        assertThat(result.firstLogIndex()).isEmpty();
        assertThat(result.lastLogIndex()).isEmpty();
        assertThat(result.highestTerm()).isEmpty();
    }

    public void testV1FormatDetected() throws IOException {
        writeV1File();

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.exitCode()).isEqualTo(2);
        String output = formatOutput(result);
        assertThat(output).contains("v1");
        assertThat(output).contains("v2");
    }

    public void testUnrecognizedFormatDetected() throws IOException {
        Files.write(tempDir.resolve(ENTRIES_FILE), new byte[]{0x7F, 0x45, 0x4C, 0x46});

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(2);
    }

    public void testCrcMismatchDetected() throws IOException {
        try (FileChannel ch = openEntriesFile()) {
            writeFileHeader(ch);
            writeV2Entry(ch, 1, 1, "good".getBytes());
            writeV2EntryWithBadCrc(ch, 2, 2, "corrupt".getBytes());
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("CRC");
    }

    public void testIncompleteTrailingEntryDetected() throws IOException {
        try (FileChannel ch = openEntriesFile()) {
            writeFileHeader(ch);
            writeV2Entry(ch, 1, 1, "complete".getBytes());
            writeIncompleteEntry(ch, 2, 2, 64);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
    }

    public void testInvalidEntryHeader() throws IOException {
        try (FileChannel ch = openEntriesFile()) {
            writeFileHeader(ch);
            writeV2Entry(ch, 1, 1, "good".getBytes());
            writeEntryWithInvalidLength(ch, 2, 2);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
    }

    public void testScanContinuesPastCrcMismatch() throws IOException {
        try (FileChannel ch = openEntriesFile()) {
            writeFileHeader(ch);
            writeV2EntryWithBadCrc(ch, 1, 1, "bad1".getBytes());
            writeV2Entry(ch, 2, 2, "good".getBytes());
            writeV2EntryWithBadCrc(ch, 3, 3, "bad2".getBytes());
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(result.lastLogIndex()).hasValue(3);
    }

    public void testMixedLegacyAndV2Entries() throws IOException {
        try (FileChannel ch = openEntriesFile()) {
            writeFileHeader(ch);
            writeV1Entry(ch, 1, 1, "legacy".getBytes());
            writeV1Entry(ch, 2, 2, "legacy".getBytes());
            writeV2Entry(ch, 3, 3, "modern".getBytes());
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(result.firstLogIndex()).hasValue(1);
        assertThat(result.lastLogIndex()).hasValue(3);
        String output = formatOutput(result);
        assertThat(output).contains("2 entries");
        assertThat(output).containsIgnoringCase("no checksums");
    }

    private FileBasedLog createLog() throws Exception {
        FileBasedLog log = new FileBasedLog();
        log.useFsync(false);
        log.init(tempDir.toString(), null);
        return log;
    }

    private ValidationContext validate() throws IOException {
        EntriesFileRule rule = new EntriesFileRule();
        return rule.validate(tempDir.toFile(), ValidationContext.empty());
    }

    private ValidationResult singleResult(ValidationContext context) {
        assertThat(context.results()).hasSize(1);
        return context.results().get(0);
    }

    private String formatOutput(ValidationContext context) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        for (ValidationResult result : context.results()) {
            result.formatTo(pw);
        }
        pw.flush();
        return sw.toString();
    }

    private FileChannel openEntriesFile() throws IOException {
        return FileChannel.open(tempDir.resolve(ENTRIES_FILE),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    private void writeFileHeader(FileChannel ch) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_SIZE);
        header.put(RAFT_MAGIC);
        header.put((byte) 2);
        header.put(new byte[3]);
        header.flip();
        ch.write(header);
    }

    private void writeV2Entry(FileChannel ch, long term, long index, byte[] data) throws IOException {
        int totalLength = ENTRY_HEADER_SIZE + data.length + CRC_SIZE;
        byte[] headerBytes = entryHeader(V2_ENTRY_MAGIC, totalLength, term, index, data.length);

        CRC32C crc = new CRC32C();
        crc.update(headerBytes);
        crc.update(data);

        ByteBuffer crcBuf = ByteBuffer.allocate(CRC_SIZE);
        crcBuf.putInt((int) crc.getValue());
        crcBuf.flip();

        ch.write(ByteBuffer.wrap(headerBytes));
        ch.write(ByteBuffer.wrap(data));
        ch.write(crcBuf);
    }

    private void writeV2EntryWithBadCrc(FileChannel ch, long term, long index, byte[] data) throws IOException {
        int totalLength = ENTRY_HEADER_SIZE + data.length + CRC_SIZE;
        byte[] headerBytes = entryHeader(V2_ENTRY_MAGIC, totalLength, term, index, data.length);

        ByteBuffer crcBuf = ByteBuffer.allocate(CRC_SIZE);
        crcBuf.putInt(0xDEADBEEF);
        crcBuf.flip();

        ch.write(ByteBuffer.wrap(headerBytes));
        ch.write(ByteBuffer.wrap(data));
        ch.write(crcBuf);
    }

    private void writeV1Entry(FileChannel ch, long term, long index, byte[] data) throws IOException {
        int totalLength = ENTRY_HEADER_SIZE + data.length;
        byte[] headerBytes = entryHeader(V1_ENTRY_MAGIC, totalLength, term, index, data.length);

        ch.write(ByteBuffer.wrap(headerBytes));
        ch.write(ByteBuffer.wrap(data));
    }

    private void writeIncompleteEntry(FileChannel ch, long term, long index, int declaredDataLength) throws IOException {
        int totalLength = ENTRY_HEADER_SIZE + declaredDataLength + CRC_SIZE;
        byte[] headerBytes = entryHeader(V2_ENTRY_MAGIC, totalLength, term, index, declaredDataLength);

        ch.write(ByteBuffer.wrap(headerBytes));
        ch.write(ByteBuffer.wrap(new byte[declaredDataLength / 2]));
    }

    private void writeEntryWithInvalidLength(FileChannel ch, long term, long index) throws IOException {
        int badDataLength = Integer.MAX_VALUE - ENTRY_HEADER_SIZE - CRC_SIZE;
        byte[] headerBytes = entryHeader(V2_ENTRY_MAGIC, Integer.MAX_VALUE, term, index, badDataLength);

        ch.write(ByteBuffer.wrap(headerBytes));
    }

    private byte[] entryHeader(byte magic, int totalLength, long term, long index, int dataLength) {
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_HEADER_SIZE);
        buf.put(magic);
        buf.putInt(totalLength);
        buf.putLong(term);
        buf.putLong(index);
        buf.put((byte) 0x00);
        buf.putInt(dataLength);
        return buf.array();
    }

    private void writeV1File() throws IOException {
        try (FileChannel ch = openEntriesFile()) {
            writeV1Entry(ch, 1, 1, new byte[10]);
        }
    }
}
