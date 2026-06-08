package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;

import org.jgroups.raft.filelog.LogDirectoryLock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class FileBasedLogIntegrationTest {

    private static final int FILE_HEADER_SIZE = 8;

    private Path tempDir;
    private FileBasedLog log;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("filebased-log-integration-test");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (log != null) {
            log.close();
        }
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testSnapshotAndLogPersistAcrossReopen() throws Exception {
        log = createLog();

        byte[] buf = new byte[10];
        append(1, buf, 1, 1, 1, 2, 2);
        log.commitIndex(5);
        log.setSnapshot(ByteBuffer.wrap("snapshot-at-5".getBytes()));

        log.close();
        log = createLog();

        assertIndices(0, 5, 5, 2);
        ByteBuffer snapshot = log.getSnapshot();
        assertThat(snapshot).isNotNull();
        byte[] snapshotData = new byte[snapshot.remaining()];
        snapshot.get(snapshotData);
        assertThat(new String(snapshotData)).isEqualTo("snapshot-at-5");

        for (int i = 1; i <= 5; i++) {
            assertThat(log.get(i)).isNotNull();
        }

        append(6, buf, 3);
        log.close();
        log = createLog();

        assertIndices(0, 6, 5, 3);
        assertThat(log.get(6)).isNotNull();
    }

    public void testCorruptedSnapshotDoesNotCorruptLog() throws Exception {
        log = createLog();

        byte[] buf = "entry-data".getBytes();
        append(1, buf, 1, 1, 1);
        log.commitIndex(3);
        log.setSnapshot(ByteBuffer.wrap("good-snapshot".getBytes()));
        log.close();

        corruptByteAt(snapshotPath(), 12);

        log = createLog();

        assertThatThrownBy(() -> log.getSnapshot())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC");

        SoftAssertions.assertSoftly(softly -> {
            for (int i = 1; i <= 3; i++) {
                try {
                    softly.assertThat(log.get(i)).as("Entry %d", i).isNotNull();
                } catch (IOException e) {
                    softly.fail(e);
                }
            }
        });

        append(4, "new-entry".getBytes(), 2);
        assertThat(log.lastAppended()).isEqualTo(4);
        assertThat(log.get(4)).isNotNull();
    }

    public void testReinitializeToUpdatesAllStoragesConsistently() throws Exception {
        log = createLog();

        byte[] buf = new byte[10];
        append(1, buf, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5);
        log.commitIndex(10);

        LogEntry snapshotEntry = new LogEntry(7, "snapshot-entry".getBytes());
        log.reinitializeTo(5, snapshotEntry);

        assertIndices(5, 5, 5, 7);
        assertThat(log.size()).isEqualTo(1);

        log.close();
        log = createLog();

        assertIndices(5, 5, 5, 7);
        assertThat(log.size()).isEqualTo(1);

        LogEntry loaded = log.get(5);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(7);
        byte[] command = new byte[loaded.length()];
        System.arraycopy(loaded.command(), loaded.offset(), command, 0, loaded.length());
        assertThat(new String(command)).isEqualTo("snapshot-entry");

        for (int i = 1; i <= 4; i++) {
            assertThat(log.get(i)).as("Entry %d should be null after reinitialize", i).isNull();
        }

        append(6, buf, 8, 8);
        assertIndices(5, 7, 5, 8);
    }

    public void testReinitializeToWithNullCommandSurvivesReopen() throws Exception {
        log = createLog();

        byte[] buf = new byte[10];
        append(1, buf, 1, 1, 2, 2, 3);
        log.commitIndex(5);

        log.reinitializeTo(5, new LogEntry(3, null));

        assertIndices(5, 5, 5, 3);

        log.close();
        log = createLog();

        assertIndices(5, 5, 5, 3);

        LogEntry loaded = log.get(5);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(3);
        assertThat(loaded.length()).isZero();

        for (int i = 1; i <= 4; i++) {
            assertThat(log.get(i)).as("Entry %d should be null after reinitialize", i).isNull();
        }
    }

    public void testReinitializeToWithNullCommandThenAppendSurvivesReopen() throws Exception {
        log = createLog();

        byte[] buf = "data".getBytes();
        append(1, buf, 1, 1, 2);
        log.commitIndex(3);

        log.reinitializeTo(3, new LogEntry(2, null));

        append(4, buf, 2, 3);
        log.commitIndex(5);

        log.close();
        log = createLog();

        assertIndices(3, 5, 5, 3);

        LogEntry snap = log.get(3);
        assertThat(snap).isNotNull();
        assertThat(snap.term()).isEqualTo(2);
        assertThat(snap.length()).isZero();

        assertThat(log.get(4)).isNotNull();
        assertThat(log.get(5)).isNotNull();
    }

    public void testForEachOverNullCommandEntryAfterReopen() throws Exception {
        log = createLog();

        log.reinitializeTo(10, new LogEntry(4, null));
        append(11, "after".getBytes(), 4, 5);
        log.commitIndex(12);

        log.close();
        log = createLog();

        int[] count = {0};
        log.forEach((entry, index) -> {
            if (index == 10) {
                assertThat(entry.term()).isEqualTo(4);
                assertThat(entry.length()).isZero();
            }
            count[0]++;
        }, 10, 12);
        assertThat(count[0]).isEqualTo(3);
    }

    public void testInitCreatesLockFile() throws Exception {
        log = createLog();

        assertThat(tempDir.resolve("raft.lock")).exists();
    }

    public void testRunningLogHoldsLock() throws Exception {
        log = createLog();

        assertThat(tempDir.resolve("raft.lock")).exists();

        try (LogDirectoryLock externalLock = new LogDirectoryLock(tempDir.toFile())) {
            assertThatThrownBy(externalLock::tryAcquire)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("already locked by the current JVM");
        }
    }

    public void testInitRefusesLockedDirectory() throws Exception {
        try (LogDirectoryLock externalLock = new LogDirectoryLock(tempDir.toFile())) {
            externalLock.tryAcquire();

            FileBasedLog blockedLog = new FileBasedLog();
            assertThatThrownBy(() -> blockedLog.init(tempDir.toString(), null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("locked");
        }
    }

    public void testCloseReleasesLockFile() throws Exception {
        log = createLog();
        log.close();
        log = null;

        try (LogDirectoryLock afterClose = new LogDirectoryLock(tempDir.toFile())) {
            assertThat(afterClose.tryAcquire()).isTrue();
        }
    }

    public void testInitFailsOnCorruptedLogFile() throws Exception {
        log = createLog();

        byte[] buf = new byte[10];
        append(1, buf, 1, 1, 1);
        log.close();

        long termPosition = FILE_HEADER_SIZE + 1 + 4;
        corruptByteAt(entriesPath(), termPosition);

        FileBasedLog corruptedLog = new FileBasedLog();
        corruptedLog.useFsync(false);
        assertThatThrownBy(() -> corruptedLog.init(tempDir.toString(), null))
                .isInstanceOf(IOException.class);
    }

    private FileBasedLog createLog() throws Exception {
        FileBasedLog fbl = new FileBasedLog();
        fbl.useFsync(false);
        fbl.init(tempDir.toString(), null);
        return fbl;
    }

    private void append(int startIndex, byte[] buf, int... terms) throws Exception {
        LogEntries le = new LogEntries();
        for (int term : terms) {
            le.add(new LogEntry(term, buf));
        }
        log.append(startIndex, le);
    }

    private void assertIndices(int firstAppended, int lastAppended, int commitIndex, int currentTerm) {
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(log.firstAppended()).as("firstAppended").isEqualTo(firstAppended);
            softly.assertThat(log.lastAppended()).as("lastAppended").isEqualTo(lastAppended);
            softly.assertThat(log.commitIndex()).as("commitIndex").isEqualTo(commitIndex);
            softly.assertThat(log.currentTerm()).as("currentTerm").isEqualTo(currentTerm);
        });
    }

    private Path entriesPath() {
        return tempDir.resolve("entries.raft");
    }

    private Path snapshotPath() {
        return tempDir.resolve("state_snapshot.raft");
    }

    private void corruptByteAt(Path file, long position) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(position);
            byte original = raf.readByte();
            raf.seek(position);
            raf.writeByte(original ^ 0xFF);
        }
    }
}
