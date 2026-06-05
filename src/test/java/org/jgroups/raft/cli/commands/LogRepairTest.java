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
import org.jgroups.raft.filelog.MetadataStorage;
import org.jgroups.raft.filelog.SnapshotStorage;

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
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.commitIndex()).isEqualTo(1);
        });
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

    public void testVotedForCorruptedCleared() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(2, d1),
                    new LogEntry(2, d2)
            ));
            log.currentTerm(2);
            log.commitIndex(2);
        }

        // Write garbage at the votedFor position (offset 16) to trigger deserialization failure.
        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.seek(16);
            raf.writeInt(-1);
            raf.write(new byte[] {0x01, 0x02, 0x03});
        }

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("vote");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.commitIndex()).isEqualTo(2);
            assertThat(readable.currentTerm()).isEqualTo(2);
        });
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(2);
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testTruncatedMetadataReconstructed() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(3, d1),
                    new LogEntry(5, d2)
            ));
            log.currentTerm(5);
            log.commitIndex(2);
        }

        // Truncate metadata below the 16-byte minimum. Only partial commitIndex survives.
        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.setLength(5);
        }

        ValidationResult preRepair = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(preRepair.metadataInfo()).hasValueSatisfying(meta ->
                assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Truncated.class));

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("reconstruct");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.commitIndex()).isEqualTo(0);
            assertThat(readable.currentTerm()).isEqualTo(5);
        });
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testCommitIndexBeyondLogRangeStandaloneAdjusted() throws Exception {
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

        // Overwrite commitIndex to 100, well beyond the last log entry (3).
        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.seek(0);
            raf.writeLong(100);
        }

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("commit index");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.commitIndex()).isEqualTo(3);
            assertThat(readable.currentTerm()).isEqualTo(1);
        });
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.entryCount()).isEqualTo(3);
        });
    }

    public void testTermInconsistencyAdjusted() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(3, d1),
                    new LogEntry(5, d2)
            ));
            log.currentTerm(5);
            log.commitIndex(2);
        }

        // Overwrite currentTerm to 1, below the highest entry term (5).
        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.seek(8);
            raf.writeLong(1);
        }

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("term");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.currentTerm()).isEqualTo(5);
            assertThat(readable.commitIndex()).isEqualTo(2);
        });
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testMetadataRepairDeclined() throws Exception {
        byte[] d1 = "data".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(new LogEntry(2, d1)));
            log.currentTerm(2);
        }

        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.setLength(5);
        }

        long sizeBefore = Files.size(metadataPath());

        RepairResult result = executeRepair("yes", "no");

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(Files.size(metadataPath())).isEqualTo(sizeBefore);
    }

    public void testCorruptVotedForAndTermInconsistencyRepairedTogether() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(3, d1),
                    new LogEntry(5, d2)
            ));
            log.currentTerm(5);
            log.commitIndex(2);
        }

        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.seek(8);
            raf.writeLong(1);
            raf.seek(16);
            raf.writeInt(-1);
            raf.write(new byte[] {0x01, 0x02, 0x03});
        }

        RepairResult result = executeRepair("yes", "yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("vote");
        assertThat(result.output).containsIgnoringCase("term");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.commitIndex()).isEqualTo(2);
            assertThat(readable.currentTerm()).isEqualTo(5);
            assertThat(readable.voteStatus()).isEqualTo(ValidationResult.MetadataInfo.VoteStatus.ABSENT);
        });
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testEntryCrcMismatchAndCorruptVotedForRepairedTogether() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        byte[] d3 = "third".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(2, d1),
                    new LogEntry(2, d2),
                    new LogEntry(2, d3)
            ));
            log.currentTerm(2);
            log.commitIndex(3);
        }

        long corruptOffset = entryDataOffset(1, d1.length);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.seek(16);
            raf.writeInt(-1);
            raf.write(new byte[] {0x01, 0x02, 0x03});
        }

        RepairResult result = executeRepair("yes", "yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.firstIndex()).isEqualTo(1);
            assertThat(info.lastIndex()).isEqualTo(1);
            assertThat(info.entryCount()).isEqualTo(1);
        });
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.commitIndex()).isEqualTo(1);
            assertThat(readable.currentTerm()).isEqualTo(2);
            assertThat(readable.voteStatus()).isEqualTo(ValidationResult.MetadataInfo.VoteStatus.ABSENT);
        });
    }

    public void testSecondActionDeclinedFirstPersists() throws Exception {
        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();
        try (FileBasedLog log = createLog()) {
            log.append(1, LogEntries.create(
                    new LogEntry(3, d1),
                    new LogEntry(5, d2)
            ));
            log.currentTerm(5);
            log.commitIndex(2);
        }

        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.seek(8);
            raf.writeLong(1);
            raf.seek(16);
            raf.writeInt(-1);
            raf.write(new byte[] {0x01, 0x02, 0x03});
        }

        RepairResult result = executeRepair("yes", "yes", "no");

        assertThat(result.exitCode).isEqualTo(1);

        assertThat(Files.size(metadataPath())).isEqualTo(16);

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isFalse();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.currentTerm()).isEqualTo(1);
            assertThat(readable.voteStatus()).isEqualTo(ValidationResult.MetadataInfo.VoteStatus.ABSENT);
        });
        assertThat(verification.logInfo()).hasValueSatisfying(info -> {
            assertThat(info.entryCount()).isEqualTo(2);
        });
    }

    public void testTruncatedMetadataReconstructedWithEmptyLog() throws Exception {
        try (FileBasedLog log = createLog()) {
            // No entries appended. Metadata exists with default values.
        }

        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.setLength(5);
        }

        ValidationResult preRepair = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(preRepair.metadataInfo()).hasValueSatisfying(meta ->
                assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Truncated.class));

        RepairResult result = executeRepair("yes", "yes");

        assertThat(result.exitCode).isEqualTo(0);
        assertThat(result.output).containsIgnoringCase("reconstruct");

        ValidationResult verification = LogValidation.validate(tempDir.toFile(), LogValidationOptions.simple());
        assertThat(verification.isValid()).isTrue();
        assertThat(verification.metadataInfo()).hasValueSatisfying(meta -> {
            assertThat(meta).isInstanceOf(ValidationResult.MetadataInfo.Readable.class);
            ValidationResult.MetadataInfo.Readable readable = (ValidationResult.MetadataInfo.Readable) meta;
            assertThat(readable.commitIndex()).isEqualTo(0);
            assertThat(readable.currentTerm()).isEqualTo(0);
        });
    }

    public void testCorruptSnapshotWithDependentLogNonRepairable() throws Exception {
        byte[] snapshotData = "state-machine-snapshot".getBytes();
        byte[] d1 = "after-snapshot".getBytes();
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(java.nio.ByteBuffer.wrap(snapshotData));
            log.reinitializeTo(501, new LogEntry(5, d1));
            log.currentTerm(5);
            log.commitIndex(501);
        }

        try (RandomAccessFile raf = new RandomAccessFile(snapshotPath().toFile(), "rw")) {
            raf.seek(SnapshotStorage.SNAPSHOT_HEADER_SIZE);
            raf.writeByte(0xFF);
        }

        long entriesSizeBefore = Files.size(entriesPath());
        long snapshotSizeBefore = Files.size(snapshotPath());

        RepairResult result = executeRepair();

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.output).containsIgnoringCase("cannot be repaired");
        assertThat(result.output).containsIgnoringCase("snapshot");
        assertThat(result.output).containsIgnoringCase("delete the entire log directory");

        assertThat(Files.size(entriesPath())).isEqualTo(entriesSizeBefore);
        assertThat(Files.size(snapshotPath())).isEqualTo(snapshotSizeBefore);
    }

    public void testTruncatedSnapshotWithDependentLogNonRepairable() throws Exception {
        byte[] snapshotData = "state-machine-snapshot-data".getBytes();
        byte[] d1 = "after-snapshot".getBytes();
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(java.nio.ByteBuffer.wrap(snapshotData));
            log.reinitializeTo(501, new LogEntry(5, d1));
            log.currentTerm(5);
            log.commitIndex(501);
        }

        try (RandomAccessFile raf = new RandomAccessFile(snapshotPath().toFile(), "rw")) {
            raf.setLength(SnapshotStorage.SNAPSHOT_HEADER_SIZE);
        }

        RepairResult result = executeRepair();

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.output).containsIgnoringCase("cannot be repaired");
        assertThat(result.output).containsIgnoringCase("snapshot");
    }

    public void testCorruptSnapshotWithEntryCorruptionStillNonRepairable() throws Exception {
        byte[] snapshotData = "state-machine-snapshot".getBytes();
        byte[] d1 = "first-after".getBytes();
        byte[] d2 = "second-after".getBytes();
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(java.nio.ByteBuffer.wrap(snapshotData));
            log.reinitializeTo(501, new LogEntry(5, d1));
            log.append(502, LogEntries.create(new LogEntry(5, d2)));
            log.currentTerm(5);
            log.commitIndex(502);
        }

        try (RandomAccessFile raf = new RandomAccessFile(snapshotPath().toFile(), "rw")) {
            raf.seek(SnapshotStorage.SNAPSHOT_HEADER_SIZE);
            raf.writeByte(0xFF);
        }

        long corruptOffset = entryDataOffset(0);
        try (RandomAccessFile raf = new RandomAccessFile(entriesPath().toFile(), "rw")) {
            raf.seek(corruptOffset);
            raf.writeByte(0xFF);
        }

        RepairResult result = executeRepair();

        assertThat(result.exitCode).isEqualTo(1);
        assertThat(result.output).containsIgnoringCase("cannot be repaired");
        assertThat(result.output).containsIgnoringCase("snapshot");
    }

    public void testHealthySnapshotWithEntryCorruptionProceedsNormally() throws Exception {
        byte[] snapshotData = "state-machine-snapshot".getBytes();
        byte[] d1 = "first-after".getBytes();
        byte[] d2 = "second-after".getBytes();
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(java.nio.ByteBuffer.wrap(snapshotData));
            log.reinitializeTo(501, new LogEntry(5, d1));
            log.append(502, LogEntries.create(new LogEntry(5, d2)));
            log.currentTerm(5);
            log.commitIndex(502);
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
            assertThat(info.firstIndex()).isEqualTo(501);
            assertThat(info.lastIndex()).isEqualTo(501);
            assertThat(info.entryCount()).isEqualTo(1);
        });
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

    private Path metadataPath() {
        return tempDir.resolve(MetadataStorage.FILE_NAME);
    }

    private Path snapshotPath() {
        return tempDir.resolve(SnapshotStorage.SNAPSHOT_FILE_NAME);
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
