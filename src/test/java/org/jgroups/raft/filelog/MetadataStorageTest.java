package org.jgroups.raft.filelog;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class MetadataStorageTest {

    private static final Address A = Util.createRandomAddress("A");
    private static final Address B = Util.createRandomAddress("B");

    private Path tempDir;
    private FileStorage fileStorage;
    private MetadataStorage storage;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("metadata-storage-test");
        storage = createStorage();
        storage.open();
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

    public void testInitialCommitIndexIsZero() {
        assertThat(storage.getCommitIndex()).isZero();
    }

    public void testInitialCurrentTermIsZero() {
        assertThat(storage.getCurrentTerm()).isZero();
    }

    public void testInitialVotedForIsNull() throws Exception {
        assertThat(storage.getVotedFor()).isNull();
    }

    public void testSetAndGetCommitIndex() throws IOException {
        storage.setCommitIndex(42);
        assertThat(storage.getCommitIndex()).isEqualTo(42);
    }

    public void testSetAndGetCurrentTerm() throws IOException {
        storage.setCurrentTerm(5);
        assertThat(storage.getCurrentTerm()).isEqualTo(5);
    }

    public void testSetAndGetVotedFor() throws Exception {
        storage.setVotedFor(A);
        assertThat(storage.getVotedFor()).isEqualTo(A);
    }

    public void testClearVotedFor() throws Exception {
        storage.setVotedFor(A);
        storage.setVotedFor(null);
        assertThat(storage.getVotedFor()).isNull();
    }

    public void testClearVotedForOnFreshStorage() throws Exception {
        storage.setVotedFor(null);
        assertThat(storage.getVotedFor()).isNull();
    }

    public void testOverwriteVotedFor() throws Exception {
        storage.setVotedFor(A);
        storage.setVotedFor(B);
        assertThat(storage.getVotedFor()).isEqualTo(B);
    }

    public void testOverwriteCurrentTerm() throws IOException {
        storage.setCurrentTerm(1);
        storage.setCurrentTerm(5);
        storage.setCurrentTerm(10);
        assertThat(storage.getCurrentTerm()).isEqualTo(10);
    }

    public void testSetVotedForDoesNotAffectOtherFields() throws Exception {
        storage.setCommitIndex(100);
        storage.setCurrentTerm(7);

        storage.setVotedFor(A);

        assertThat(storage.getCommitIndex()).isEqualTo(100);
        assertThat(storage.getCurrentTerm()).isEqualTo(7);
    }

    public void testSetTermDoesNotAffectOtherFields() throws Exception {
        storage.setCommitIndex(50);
        storage.setVotedFor(A);

        storage.setCurrentTerm(99);

        assertThat(storage.getCommitIndex()).isEqualTo(50);
        assertThat(storage.getVotedFor()).isEqualTo(A);
    }

    public void testClearVotedForDoesNotAffectOtherFields() throws Exception {
        storage.setCommitIndex(50);
        storage.setCurrentTerm(7);
        storage.setVotedFor(A);

        storage.setVotedFor(null);

        assertThat(storage.getCommitIndex()).isEqualTo(50);
        assertThat(storage.getCurrentTerm()).isEqualTo(7);
    }

    public void testAllFieldsPersistAcrossReopen() throws Exception {
        storage.setCommitIndex(42);
        storage.setCurrentTerm(7);
        storage.setVotedFor(A);

        reopen();

        assertThat(storage.getCommitIndex()).isEqualTo(42);
        assertThat(storage.getCurrentTerm()).isEqualTo(7);
        assertThat(storage.getVotedFor()).isEqualTo(A);
    }

    public void testNullVotedForPersistsAcrossReopen() throws Exception {
        storage.setVotedFor(A);
        storage.setVotedFor(null);

        reopen();

        assertThat(storage.getVotedFor()).isNull();
    }

    public void testOverwrittenVotedForPersistsAcrossReopen() throws Exception {
        storage.setVotedFor(A);
        storage.setVotedFor(B);

        reopen();

        assertThat(storage.getVotedFor()).isEqualTo(B);
    }

    public void testTermAndVoteEvolutionPersists() throws Exception {
        storage.setCurrentTerm(1);
        storage.setVotedFor(A);
        storage.setCurrentTerm(2);
        storage.setVotedFor(null);
        storage.setCurrentTerm(3);
        storage.setVotedFor(B);

        reopen();

        assertThat(storage.getCurrentTerm()).isEqualTo(3);
        assertThat(storage.getVotedFor()).isEqualTo(B);
    }

    public void testSetVotedForFlushes() throws Exception {
        storage.setVotedFor(A);
        assertThat(fileStorage.hasPendingFlush()).isFalse();
    }

    public void testClearVotedForFlushes() throws Exception {
        storage.setVotedFor(A);
        storage.setVotedFor(null);
        assertThat(fileStorage.hasPendingFlush()).isFalse();
    }

    public void testSetVotedForDoesNotFlushWhenFsyncDisabled() throws Exception {
        storage.useFsync(false);
        storage.setVotedFor(A);
        assertThat(fileStorage.hasPendingFlush()).isTrue();
    }

    public void testTruncatedVotedForReturnsNull() throws Exception {
        storage.setVotedFor(A);
        storage.close();

        try (RandomAccessFile raf = new RandomAccessFile(metadataFile(), "rw")) {
            raf.setLength(16);
        }

        storage = createStorage();
        storage.open();
        assertThat(storage.getVotedFor()).isNull();
    }

    public void testOversizedAddressLengthReturnsNull() throws Exception {
        storage.setVotedFor(A);
        storage.close();

        long fileLength;
        try (RandomAccessFile raf = new RandomAccessFile(metadataFile(), "rw")) {
            fileLength = raf.length();
            raf.seek(16);
            raf.writeInt((int) fileLength);
        }

        storage = createStorage();
        storage.open();
        assertThat(storage.getVotedFor()).isNull();
    }

    public void testFixedFieldsSurviveVoteCorruption() throws Exception {
        storage.setCommitIndex(99);
        storage.setCurrentTerm(12);
        storage.setVotedFor(A);
        storage.close();

        try (RandomAccessFile raf = new RandomAccessFile(metadataFile(), "rw")) {
            raf.setLength(16);
        }

        storage = createStorage();
        storage.open();
        assertThat(storage.getCommitIndex()).isEqualTo(99);
        assertThat(storage.getCurrentTerm()).isEqualTo(12);
        assertThat(storage.getVotedFor()).isNull();
    }

    private MetadataStorage createStorage() {
        fileStorage = new FileStorage(metadataFile());
        return new MetadataStorage(fileStorage, true);
    }

    private void reopen() throws IOException {
        storage.close();
        storage = createStorage();
        storage.open();
    }

    private File metadataFile() {
        return tempDir.resolve("metadata.raft").toFile();
    }
}
