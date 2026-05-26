package org.jgroups.raft.filelog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LogDirectoryLockTest {

    private Path tempDir;
    private LogDirectoryLock lock;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log-directory-lock-test");
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (lock != null) {
            lock.close();
        }
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testAcquireSucceedsOnUnlockedDirectory() throws IOException {
        lock = new LogDirectoryLock(tempDir.toFile());
        boolean acquired = lock.tryAcquire();

        assertThat(acquired).isTrue();
        assertThat(tempDir.resolve("raft.lock")).exists();
    }

    public void testCloseReleasesLock() throws IOException {
        lock = new LogDirectoryLock(tempDir.toFile());
        lock.tryAcquire();
        lock.close();
        lock = null;

        try (LogDirectoryLock second = new LogDirectoryLock(tempDir.toFile())) {
            assertThat(second.tryAcquire()).isTrue();
        }
    }

    public void testSameJvmDoubleAcquireThrowsIOException() throws IOException {
        lock = new LogDirectoryLock(tempDir.toFile());
        lock.tryAcquire();

        try (LogDirectoryLock second = new LogDirectoryLock(tempDir.toFile())) {
            assertThatThrownBy(second::tryAcquire)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("already locked by the current JVM");
        }
    }

    public void testCloseIsIdempotent() throws IOException {
        lock = new LogDirectoryLock(tempDir.toFile());
        lock.tryAcquire();
        lock.close();
        lock.close();
        lock = null;
    }

    public void testCloseWithoutAcquireIsSafe() throws IOException {
        lock = new LogDirectoryLock(tempDir.toFile());
        lock.close();
        lock = null;
    }
}
