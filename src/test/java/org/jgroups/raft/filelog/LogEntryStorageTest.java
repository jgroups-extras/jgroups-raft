package org.jgroups.raft.filelog;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;

import java.io.File;
import java.io.IOException;
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
public class LogEntryStorageTest {

    private Path tempDir;
    private LogEntryStorage storage;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log-entry-storage-test");
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

    public void testWriteSingleEntryPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(3, "hello"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(1);
        LogEntry loaded = storage.getLogEntry(1);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(3);
        assertThat(new String(loaded.command())).isEqualTo("hello");
    }

    public void testWriteBatchPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "one"), entry(2, "two"), entry(3, "three"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isZero();
        assertThat(storage.getLastAppended()).isEqualTo(3);

        LogEntry first = storage.getLogEntry(1);
        assertThat(first).isNotNull();
        assertThat(first.term()).isEqualTo(1);
        assertThat(new String(first.command())).isEqualTo("one");

        LogEntry second = storage.getLogEntry(2);
        assertThat(second).isNotNull();
        assertThat(second.term()).isEqualTo(2);
        assertThat(new String(second.command())).isEqualTo("two");

        LogEntry third = storage.getLogEntry(3);
        assertThat(third).isNotNull();
        assertThat(third.term()).isEqualTo(3);
        assertThat(new String(third.command())).isEqualTo("three");
    }

    public void testReinitializeToPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "will-be-replaced"), entry(1, "also-replaced"));
        storage.reinitializeTo(5, entry(3, "snapshot"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(5);
        assertThat(storage.getLastAppended()).isEqualTo(5);

        LogEntry loaded = storage.getLogEntry(5);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(3);
        assertThat(new String(loaded.command())).isEqualTo("snapshot");

        assertThat(storage.getLogEntry(1)).isNull();
        assertThat(storage.getLogEntry(2)).isNull();
    }

    public void testAppendAfterReinitializeToPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        storage.reinitializeTo(10, entry(5, "snap"));
        writeEntries(11, entry(5, "after-snap-1"), entry(6, "after-snap-2"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(10);
        assertThat(storage.getLastAppended()).isEqualTo(12);

        LogEntry snap = storage.getLogEntry(10);
        assertThat(snap).isNotNull();
        assertThat(new String(snap.command())).isEqualTo("snap");

        LogEntry after1 = storage.getLogEntry(11);
        assertThat(after1).isNotNull();
        assertThat(new String(after1.command())).isEqualTo("after-snap-1");

        LogEntry after2 = storage.getLogEntry(12);
        assertThat(after2).isNotNull();
        assertThat(after2.term()).isEqualTo(6);
        assertThat(new String(after2.command())).isEqualTo("after-snap-2");
    }

    public void testRemoveNewPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "keep-1"), entry(1, "keep-2"), entry(2, "remove-me"));

        storage.removeNew(3);
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(2);
        assertThat(storage.getLogEntry(3)).isNull();

        LogEntry kept = storage.getLogEntry(2);
        assertThat(kept).isNotNull();
        assertThat(new String(kept.command())).isEqualTo("keep-2");
    }

    public void testRemoveOldPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "old-1"), entry(1, "old-2"), entry(2, "keep-3"), entry(2, "keep-4"));

        storage.removeOld(3);
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(3);
        assertThat(storage.getLastAppended()).isEqualTo(4);
        assertThat(storage.getLogEntry(1)).isNull();
        assertThat(storage.getLogEntry(2)).isNull();

        LogEntry kept3 = storage.getLogEntry(3);
        assertThat(kept3).isNotNull();
        assertThat(new String(kept3.command())).isEqualTo("keep-3");

        LogEntry kept4 = storage.getLogEntry(4);
        assertThat(kept4).isNotNull();
        assertThat(new String(kept4.command())).isEqualTo("keep-4");
    }

    public void testOverwriteConflictPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "a"), entry(1, "b"), entry(1, "c"), entry(1, "d"), entry(1, "e"));
        writeEntries(3, entry(2, "X"), entry(2, "Y"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(4);

        assertThat(new String(storage.getLogEntry(1).command())).isEqualTo("a");
        assertThat(new String(storage.getLogEntry(2).command())).isEqualTo("b");
        assertThat(new String(storage.getLogEntry(3).command())).isEqualTo("X");
        assertThat(new String(storage.getLogEntry(4).command())).isEqualTo("Y");
        assertThat(storage.getLogEntry(5)).isNull();
    }

    public void testZeroLengthEntryPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, new LogEntry(7, null));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getLastAppended()).isEqualTo(1);
        LogEntry loaded = storage.getLogEntry(1);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(7);
        assertThat(loaded.length()).isZero();
    }

    public void testInternalFlagPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        LogEntry internalEntry = new LogEntry(1, "internal-cmd".getBytes()).internal(true);
        LogEntry normalEntry = new LogEntry(1, "normal-cmd".getBytes());
        writeEntries(1, internalEntry, normalEntry);
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        LogEntry loadedInternal = storage.getLogEntry(1);
        assertThat(loadedInternal).isNotNull();
        assertThat(loadedInternal.internal()).isTrue();
        assertThat(new String(loadedInternal.command())).isEqualTo("internal-cmd");

        LogEntry loadedNormal = storage.getLogEntry(2);
        assertThat(loadedNormal).isNotNull();
        assertThat(loadedNormal.internal()).isFalse();
        assertThat(new String(loadedNormal.command())).isEqualTo("normal-cmd");
    }

    public void testGetLogEntryOutOfRange() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        writeEntries(1, entry(1, "a"), entry(1, "b"));

        assertThat(storage.getLogEntry(0)).isNull();
        assertThat(storage.getLogEntry(3)).isNull();
        assertThat(storage.getLogEntry(100)).isNull();
    }

    public void testForEachOnEmptyStorage() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        List<LogEntry> collected = new ArrayList<>();
        storage.forEach((e, idx) -> collected.add(e), 1, 10);

        assertThat(collected).isEmpty();
    }

    public void testMultipleReinitializeToPersists() throws IOException {
        storage = createStorage();
        storage.open();
        storage.reload();

        storage.reinitializeTo(5, entry(2, "first-snapshot"));
        storage.reinitializeTo(20, entry(8, "second-snapshot"));
        storage.close();

        storage = createStorage();
        storage.open();
        storage.reload();

        assertThat(storage.getFirstAppended()).isEqualTo(20);
        assertThat(storage.getLastAppended()).isEqualTo(20);

        LogEntry loaded = storage.getLogEntry(20);
        assertThat(loaded).isNotNull();
        assertThat(loaded.term()).isEqualTo(8);
        assertThat(new String(loaded.command())).isEqualTo("second-snapshot");

        assertThat(storage.getLogEntry(5)).isNull();
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
}
