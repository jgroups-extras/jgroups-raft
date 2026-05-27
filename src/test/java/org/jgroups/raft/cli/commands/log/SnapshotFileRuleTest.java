package org.jgroups.raft.cli.commands.log;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.raft.filelog.SnapshotStorage;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SnapshotFileRuleTest {

    private Path tempDir;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("snapshot-file-rule-test");
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

    // --- Tests using real FileBasedLog ---

    public void testNoSnapshotWritten() throws Exception {
        try (FileBasedLog ignored = createLog()) {
            // no snapshot
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        assertThat(formatOutput(result)).containsIgnoringCase("MISSING");
    }

    public void testValidSnapshot() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(ByteBuffer.wrap("state-machine-snapshot-data".getBytes()));
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        assertThat(formatOutput(result)).containsIgnoringCase("checksum valid");
    }

    public void testEmptySnapshotData() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(ByteBuffer.wrap(new byte[0]));
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
    }

    public void testLargeSnapshot() throws Exception {
        byte[] data = new byte[64 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 251);
        }
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(ByteBuffer.wrap(data));
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
    }

    public void testCrcMismatch() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(ByteBuffer.wrap("valid-snapshot-data".getBytes()));
        }

        try (RandomAccessFile raf = new RandomAccessFile(snapshotPath().toFile(), "rw")) {
            raf.seek(SnapshotStorage.SNAPSHOT_HEADER_SIZE);
            raf.writeByte(0xFF);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("CRC");
    }

    public void testTruncatedFile() throws Exception {
        try (FileBasedLog log = createLog()) {
            log.setSnapshot(ByteBuffer.wrap("snapshot-to-truncate".getBytes()));
        }

        try (RandomAccessFile raf = new RandomAccessFile(snapshotPath().toFile(), "rw")) {
            raf.setLength(SnapshotStorage.SNAPSHOT_HEADER_SIZE);
        }

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(1);
        assertThat(formatOutput(result)).containsIgnoringCase("truncated");
    }

    // --- Tests requiring hand-crafted files ---

    public void testEmptyFile() throws IOException {
        Files.createFile(snapshotPath());

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isTrue();
        assertThat(fileResult.exitCode()).isEqualTo(0);
        assertThat(formatOutput(result)).containsIgnoringCase("EMPTY");
    }

    public void testUnrecognizedFormat() throws IOException {
        Files.write(snapshotPath(), new byte[]{0x7F, 0x45, 0x4C, 0x46});

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(2);
    }

    public void testUnsupportedVersion() throws IOException {
        writeSnapshotHeader((byte) 99);

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(2);
        assertThat(formatOutput(result)).contains("99");
    }

    public void testVersionZeroRejected() throws IOException {
        writeSnapshotHeader((byte) 0);

        ValidationContext result = validate();

        ValidationResult fileResult = singleResult(result);
        assertThat(fileResult.isValid()).isFalse();
        assertThat(fileResult.exitCode()).isEqualTo(2);
    }

    // --- Helpers ---

    private FileBasedLog createLog() throws Exception {
        FileBasedLog log = new FileBasedLog();
        log.useFsync(false);
        log.init(tempDir.toString(), null);
        return log;
    }

    private ValidationContext validate() throws IOException {
        SnapshotFileRule rule = new SnapshotFileRule();
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

    private Path snapshotPath() {
        return tempDir.resolve(SnapshotStorage.SNAPSHOT_FILE_NAME);
    }

    private void writeSnapshotHeader(byte version) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(SnapshotStorage.SNAPSHOT_HEADER_SIZE);
        header.put(SnapshotStorage.SNAPSHOT_HEADER_MAGIC);
        header.put(version);
        header.put(new byte[3]);
        try (ByteChannel ch = Files.newByteChannel(snapshotPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ch.write(header.flip());
        }
    }
}
