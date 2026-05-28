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
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class EntriesFileRuleTest {

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

    public void testCrcMismatchDetected() throws Exception {
        byte[] data = "payload".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, "good".getBytes()),
                    new LogEntry(2, data)
            ));
        }

        long corruptOffset = entryDataOffset(1, "good".getBytes().length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("CRC");
    }

    public void testIncompleteTrailingEntryDetected() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, "complete".getBytes()),
                    new LogEntry(2, "to-be-truncated".getBytes())
            ));
        }

        long truncateAt = entryOffset(1, "complete".getBytes().length)
                + LogEntryStorage.HEADER_SIZE + 2;
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.setLength(truncateAt);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
    }

    public void testInvalidEntryHeader() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, "good".getBytes()),
                    new LogEntry(2, "victim".getBytes())
            ));
        }

        long secondEntryOffset = entryOffset(1, "good".getBytes().length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(secondEntryOffset);
            raf.writeByte(0x7F);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
    }

    public void testScanContinuesPastCrcMismatch() throws Exception {
        byte[] d1 = "bad1".getBytes();
        byte[] d2 = "good".getBytes();
        byte[] d3 = "bad2".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(2, d2),
                    new LogEntry(3, d3)
            ));
        }

        long entry1DataOffset = entryDataOffset(0, 0);
        long entry3DataOffset = entryDataOffset(2, d1.length, d2.length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(entry1DataOffset);
            raf.writeByte(0xFF);

            raf.seek(entry3DataOffset);
            raf.writeByte(0xFF);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(result.lastLogIndex()).hasValue(3);
    }

    public void testEmptyEntriesFile() throws IOException {
        Files.createFile(entriesPath());

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
        Files.write(entriesPath(), new byte[]{0x7F, 0x45, 0x4C, 0x46});

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(2);
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

    public void testCallbackInvokedPerEntry() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, "alpha".getBytes()),
                    new LogEntry(3, "beta".getBytes())
            ));
        }

        List<CallbackRecord> entries = new ArrayList<>();
        EntryCallback collecting = (index, term, internal, dataLength, magic, status, payload) ->
                entries.add(new CallbackRecord(index, term, internal, dataLength, magic, status, new String(payload)));

        ValidationContext context = ValidationContext.withOptions(LogValidationOptions.withCallback(collecting));
        EntriesFileRule rule = new EntriesFileRule();
        rule.validate(tempDir.toFile(), context);

        assertThat(entries).hasSize(2);

        CallbackRecord first = entries.get(0);
        assertThat(first.index).isEqualTo(1);
        assertThat(first.term).isEqualTo(1);
        assertThat(first.internal).isFalse();
        assertThat(first.dataLength).isEqualTo("alpha".length());
        assertThat(first.magic).isEqualTo(LogEntryStorage.MAGIC_NUMBER_CRC);
        assertThat(first.status).isInstanceOf(EntryCallback.CrcStatus.Ok.class);
        assertThat(first.payload).isEqualTo("alpha");

        CallbackRecord second = entries.get(1);
        assertThat(second.index).isEqualTo(2);
        assertThat(second.term).isEqualTo(3);
        assertThat(second.status).isInstanceOf(EntryCallback.CrcStatus.Ok.class);
        assertThat(second.payload).isEqualTo("beta");
    }

    public void testCallbackReceivesMismatchStatus() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, "corrupt-me".getBytes())
            ));
        }

        long dataOffset = entryDataOffset(0, 0);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(dataOffset);
            raf.writeByte(0xFF);
        }

        List<CallbackRecord> entries = new ArrayList<>();
        EntryCallback collecting = (index, term, internal, dataLength, magic, status, payload) ->
                entries.add(new CallbackRecord(index, term, internal, dataLength, magic, status, new String(payload)));

        ValidationContext context = ValidationContext.withOptions(LogValidationOptions.withCallback(collecting));
        EntriesFileRule rule = new EntriesFileRule();
        rule.validate(tempDir.toFile(), context);

        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).status).isInstanceOf(EntryCallback.CrcStatus.Mismatch.class);
    }

    public void testCallbackReceivesLegacyStatus() throws IOException {
        try (FileChannel ch = openEntriesFile()) {
            writeFileHeader(ch);
            writeV1Entry(ch, 1, 1, "old".getBytes());
            writeV2Entry(ch, 2, 2, "new".getBytes());
        }

        List<CallbackRecord> entries = new ArrayList<>();
        EntryCallback collecting = (index, term, internal, dataLength, magic, status, payload) ->
                entries.add(new CallbackRecord(index, term, internal, dataLength, magic, status, new String(payload)));

        ValidationContext context = ValidationContext.withOptions(LogValidationOptions.withCallback(collecting));
        EntriesFileRule rule = new EntriesFileRule();
        rule.validate(tempDir.toFile(), context);

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).status).isInstanceOf(EntryCallback.CrcStatus.Legacy.class);
        assertThat(entries.get(0).magic).isEqualTo(LogEntryStorage.MAGIC_NUMBER);
        assertThat(entries.get(1).status).isInstanceOf(EntryCallback.CrcStatus.Ok.class);
        assertThat(entries.get(1).magic).isEqualTo(LogEntryStorage.MAGIC_NUMBER_CRC);
    }

    private record CallbackRecord(long index, long term, boolean internal, int dataLength,
                                   byte magic, EntryCallback.CrcStatus status, String payload) { }

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

    private Path entriesPath() {
        return tempDir.resolve(LogEntryStorage.FILE_NAME);
    }

    /**
     * Returns the file offset where the Nth entry starts (0-based).
     * Each preceding entry occupies HEADER_SIZE + dataLength + CRC_SIZE bytes.
     */
    private long entryOffset(int precedingEntries, int... precedingDataLengths) {
        long offset = LogEntryStorage.FILE_HEADER_SIZE;
        for (int i = 0; i < precedingEntries; i++) {
            offset += LogEntryStorage.HEADER_SIZE + precedingDataLengths[i] + LogEntryStorage.CRC_SIZE;
        }
        return offset;
    }

    /**
     * Returns the file offset of the data region for the entry after the given preceding entries.
     */
    private long entryDataOffset(int precedingEntries, int... precedingDataLengths) {
        return entryOffset(precedingEntries, precedingDataLengths) + LogEntryStorage.HEADER_SIZE;
    }

    private FileChannel openEntriesFile() throws IOException {
        return FileChannel.open(entriesPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    private void writeFileHeader(FileChannel ch) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(LogEntryStorage.FILE_HEADER_SIZE);
        header.put(LogEntryStorage.FILE_HEADER_MAGIC);
        header.put((byte) 2);
        header.put(new byte[3]);
        header.flip();
        ch.write(header);
    }

    private void writeV2Entry(FileChannel ch, long term, long index, byte[] data) throws IOException {
        int totalLength = LogEntryStorage.HEADER_SIZE + data.length + LogEntryStorage.CRC_SIZE;
        byte[] headerBytes = entryHeader(LogEntryStorage.MAGIC_NUMBER_CRC, totalLength, term, index, data.length);

        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(headerBytes);
        crc.update(data);

        ByteBuffer crcBuf = ByteBuffer.allocate(LogEntryStorage.CRC_SIZE);
        crcBuf.putInt((int) crc.getValue());
        crcBuf.flip();

        ch.write(ByteBuffer.wrap(headerBytes));
        ch.write(ByteBuffer.wrap(data));
        ch.write(crcBuf);
    }

    private void writeV1Entry(FileChannel ch, long term, long index, byte[] data) throws IOException {
        int totalLength = LogEntryStorage.HEADER_SIZE + data.length;
        byte[] headerBytes = entryHeader(LogEntryStorage.MAGIC_NUMBER, totalLength, term, index, data.length);

        ch.write(ByteBuffer.wrap(headerBytes));
        ch.write(ByteBuffer.wrap(data));
    }

    private byte[] entryHeader(byte magic, int totalLength, long term, long index, int dataLength) {
        ByteBuffer buf = ByteBuffer.allocate(LogEntryStorage.HEADER_SIZE);
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
