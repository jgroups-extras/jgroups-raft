package org.jgroups.raft.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.raft.cli.commands.log.LogValidation;
import org.jgroups.raft.cli.commands.log.LogValidationOptions;
import org.jgroups.raft.cli.commands.log.ValidationResult;
import org.jgroups.raft.filelog.LogEntryStorage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import picocli.CommandLine;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LogRepairTest {

    private Path tempDir;
    private InputStream originalIn;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log-repair-test");
        originalIn = System.in;
    }

    @AfterMethod
    public void tearDown() throws IOException {
        System.setIn(originalIn);
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testHealthyLogNothingToRepair() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, "a".getBytes()),
                    new LogEntry(1, "b".getBytes())
            ));
            log.currentTerm(1);
        }

        RepairResult result = executeRepair();

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("No further repair needed.");
    }

    public void testCrcMismatchTruncatesLog() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2),
                    new LogEntry(1, d3)
            ));
            log.currentTerm(1);
        }

        long corruptOffset = entryDataOffset(1, d1.length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(1);
            assertThat(info.entryCount()).isEqualTo(1);
        });
    }

    public void testCommitIndexAdjustedAfterTruncation() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2),
                    new LogEntry(1, d3)
            ));
            log.currentTerm(1);
            log.commitIndex(3);
        }

        long corruptOffset = entryDataOffset(1, d1.length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("commit index adjusted");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta ->
                assertThat(meta.commitIndex()).isEqualTo(1));
    }

    public void testIncompleteTrailingEntryRepaired() throws Exception {
        byte[] d1 = "complete".getBytes();
        byte[] d2 = "truncated".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2)
            ));
            log.currentTerm(1);
        }

        long truncateAt = entryOffset(1, d1.length) + LogEntryStorage.HEADER_SIZE + 2;
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.setLength(truncateAt);
        }

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(1);
            assertThat(info.entryCount()).isEqualTo(1);
        });
    }

    public void testOperatorConfirmsBackupButDeclinesRepair() throws Exception {
        byte[] d1 = "data".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(new LogEntry(1, d1)));
            log.currentTerm(1);
        }

        long corruptOffset = entryDataOffset(0, 0);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        long fileSizeBefore = Files.size(entriesPath());

        RepairResult result = executeRepair("yes", "no");

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(Files.size(entriesPath())).isEqualTo(fileSizeBefore);
    }

    public void testCorruptedHeaderReconstructed() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2),
                    new LogEntry(1, d3)
            ));
            log.currentTerm(1);
        }

        corruptFileHeader();

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).contains("Header reconstructed");
        assertThat(result.output).contains("No further repair needed");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(3);
            assertThat(info.entryCount()).isEqualTo(3);
        });
    }

    public void testCorruptedHeaderWithTruncatedLastEntry() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2),
                    new LogEntry(1, d3)
            ));
            log.currentTerm(1);
        }

        corruptFileHeader();

        // Truncate mid-way through entry 3. Simulates crash during write.
        long truncateAt = entryOffset(2, d1.length, d2.length) + LogEntryStorage.HEADER_SIZE + 2;
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.setLength(truncateAt);
        }

        RepairResult result = executeRepair("yes", "yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).contains("Header reconstructed");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(2);
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testCorruptedHeaderWithLastEntryCrcMismatch() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2),
                    new LogEntry(1, d3)
            ));
            log.currentTerm(1);
        }

        corruptFileHeader();

        long corruptOffset = entryDataOffset(2, d1.length, d2.length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        RepairResult result = executeRepair("yes", "yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).contains("Header reconstructed");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(2);
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testCorruptedHeaderWithMiddleEntryCrcMismatch() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        byte[] d4 = "fourth".getBytes();
        byte[] d5 = "fifth".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2),
                    new LogEntry(1, d3),
                    new LogEntry(1, d4),
                    new LogEntry(1, d5)
            ));
            log.currentTerm(1);
        }

        corruptFileHeader();

        // Corrupt entry 3. Entries 4 and 5 are also removed because the log cannot have gaps.
        long corruptOffset = entryDataOffset(2, d1.length, d2.length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        RepairResult result = executeRepair("yes", "yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).contains("Header reconstructed");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(2);
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testCorruptedHeaderWithFirstEntryCrcMismatch() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(1, d1),
                    new LogEntry(1, d2),
                    new LogEntry(1, d3)
            ));
            log.currentTerm(1);
        }

        corruptFileHeader();

        long corruptOffset = entryDataOffset(0);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        RepairResult result = executeRepair("yes", "yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).contains("Header reconstructed");

        // All entries removed. Only the 8-byte file header remains.
        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).isEmpty();
    }

    public void testCorruptedHeaderDeclined() throws Exception {
        byte[] d1 = "data".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(new LogEntry(1, d1)));
            log.currentTerm(1);
        }

        corruptFileHeader();

        RepairResult result = executeRepair("no");

        assertThat(result.exitCode).isEqualTo(1);

        // Header reconstruction was declined. File should still be unrecognized.
        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isFalse();
    }

    public void testOperatorDeclinesNoModification() throws Exception {
        byte[] d1 = "data".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(new LogEntry(1, d1)));
            log.currentTerm(1);
        }

        long corruptOffset = entryDataOffset(0, 0);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        long fileSizeBefore = Files.size(entriesPath());

        RepairResult result = executeRepair("no");

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(Files.size(entriesPath())).isEqualTo(fileSizeBefore);
    }

    private FileBasedLog createLog() throws Exception {
        FileBasedLog log = new FileBasedLog();
        log.useFsync(false);
        log.init(tempDir.toString(), null);
        return log;
    }

    private Path entriesPath() {
        return tempDir.resolve(LogEntryStorage.FILE_NAME);
    }

    private void corruptFileHeader() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(0);
            raf.writeInt(0xDEADBEEF);
        }
    }

    private long entryOffset(int precedingEntries, int... precedingDataLengths) {
        long offset = LogEntryStorage.FILE_HEADER_SIZE;
        for (int i = 0; i < precedingEntries; i++) {
            offset += LogEntryStorage.HEADER_SIZE + precedingDataLengths[i] + LogEntryStorage.CRC_SIZE;
        }
        return offset;
    }

    private long entryDataOffset(int precedingEntries, int... precedingDataLengths) {
        return entryOffset(precedingEntries, precedingDataLengths) + LogEntryStorage.HEADER_SIZE;
    }

    private RepairResult executeRepair(String... inputLines) {
        String input = inputLines.length > 0
                ? String.join(System.lineSeparator(), inputLines) + System.lineSeparator()
                : "";
        System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));

        StringWriter outWriter = new StringWriter();
        StringWriter errWriter = new StringWriter();
        CommandLine cmd = new CommandLine(new LogRepair());
        cmd.setOut(new PrintWriter(outWriter, true));
        cmd.setErr(new PrintWriter(errWriter, true));
        int exitCode = cmd.execute(tempDir.toString());

        return new RepairResult(exitCode, outWriter.toString(), errWriter.toString());
    }

    private record RepairResult(int exitCode, String output, String error) { }
}
