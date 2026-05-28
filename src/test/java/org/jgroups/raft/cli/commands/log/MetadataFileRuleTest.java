package org.jgroups.raft.cli.commands.log;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.raft.filelog.MetadataStorage;
import org.jgroups.util.Util;

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
import java.util.Comparator;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class MetadataFileRuleTest {

    private Path tempDir;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("metadata-file-rule-test");
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

    public void testValidMetadataDefaults() throws Exception {
        // Log created but no entries or votes written.
        createLog().close();

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        String output = formatOutput(result);
        assertThat(output).contains("Commit index");
        assertThat(output).contains("Current term");
        assertThat(output).containsIgnoringCase("OK");
    }

    public void testValidMetadataWithTermAndCommitIndex() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(5);
            log.append(1, LogEntries.create(
                    new LogEntry(5, "data".getBytes())
            ));
            log.commitIndex(1);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        String output = formatOutput(result);
        assertThat(output).contains("5");
        assertThat(output).contains("1");
        assertThat(output).containsIgnoringCase("OK");
    }

    public void testValidMetadataWithVotedFor() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(3);
            log.votedFor(Util.createRandomAddress("A"));
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        String output = formatOutput(result);
        assertThat(output).contains("Voted for");
        assertThat(output).doesNotContain("none");
    }

    public void testVotedForCleared() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(3);
            log.votedFor(Util.createRandomAddress("B"));
            log.votedFor(null);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        String output = formatOutput(result);
        assertThat(output).contains("none");
    }

    public void testTruncatedMetadataFile() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(10);
        }

        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.setLength(Global.LONG_SIZE - 1);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("truncated");
    }

    public void testCorruptedVotedForAddress() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(3);
            log.votedFor(Util.createRandomAddress("C"));
        }

        long votedForOffset = (long) Global.LONG_SIZE + Global.LONG_SIZE;
        try (RandomAccessFile raf = new RandomAccessFile(metadataPath().toFile(), "rw")) {
            raf.seek(votedForOffset);
            raf.writeInt(9999);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("UNREADABLE");
    }

    public void testCommitIndexBeyondLastLogEntry() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(5);
            log.append(1, LogEntries.create(
                    new LogEntry(5, "a".getBytes()),
                    new LogEntry(5, "b".getBytes())
            ));
            log.commitIndex(2);
        }

        ValidationContext context = ValidationContext.empty()
                .withLogRange(1, 1)
                .withHighestTerm(5);

        ValidationContext result = validateWith(context);

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        String output = formatOutput(result);
        assertThat(output).contains("2");
        assertThat(output).contains("1");
    }

    public void testTermLowerThanHighestLogEntry() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(3);
        }

        ValidationContext context = ValidationContext.empty()
                .withHighestTerm(7);

        ValidationContext result = validateWith(context);

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        String output = formatOutput(result);
        assertThat(output).contains("3");
        assertThat(output).contains("7");
    }

    public void testConsistentWithEntriesContext() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(10);
            log.append(1, LogEntries.create(
                    new LogEntry(8, "a".getBytes()),
                    new LogEntry(10, "b".getBytes())
            ));
            log.commitIndex(2);
        }

        ValidationContext context = ValidationContext.empty()
                .withLogRange(1, 2)
                .withHighestTerm(10);

        ValidationContext result = validateWith(context);

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(formatOutput(result)).containsIgnoringCase("OK");
    }

    public void testNoEntriesContextSkipsCrossFileChecks() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.currentTerm(100);
            log.commitIndex(50);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(formatOutput(result)).containsIgnoringCase("OK");
    }

    public void testMissingMetadataFile() throws IOException {
        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        assertThat(formatOutput(result)).containsIgnoringCase("MISSING");
    }

    public void testEmptyMetadataFile() throws IOException {
        Files.createFile(metadataPath());

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        assertThat(formatOutput(result)).containsIgnoringCase("EMPTY");
    }

    public void testVotedForLengthExceedsFile() throws IOException {
        try (FileChannel ch = FileChannel.open(metadataPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(Global.LONG_SIZE + Global.LONG_SIZE + Global.INT_SIZE);
            buf.putLong(0);
            buf.putLong(1);
            buf.putInt(9999);
            buf.flip();
            ch.write(buf);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("UNREADABLE");
    }

    public void testNegativeVotedForLength() throws IOException {
        try (FileChannel ch = FileChannel.open(metadataPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(Global.LONG_SIZE + Global.LONG_SIZE + Global.INT_SIZE);
            buf.putLong(0);
            buf.putLong(1);
            buf.putInt(-1);
            buf.flip();
            ch.write(buf);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("UNREADABLE");
    }

    private FileBasedLog createLog() throws Exception {
        FileBasedLog log = new FileBasedLog();
        log.useFsync(false);
        log.init(tempDir.toString(), null);
        return log;
    }

    private ValidationContext validate() throws IOException {
        return validateWith(ValidationContext.empty());
    }

    private ValidationContext validateWith(ValidationContext context) throws IOException {
        MetadataFileRule rule = new MetadataFileRule();
        return rule.validate(tempDir.toFile(), context);
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

    private Path metadataPath() {
        return tempDir.resolve(MetadataStorage.FILE_NAME);
    }
}
