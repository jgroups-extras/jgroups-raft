package org.jgroups.raft.filelog;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Exclusive lock on a Raft log directory, preventing concurrent access from multiple nodes or CLI commands.
 *
 * <p>
 * The lock is backed by a {@code raft.lock} file in the log directory, using the operating system's {@link FileLock} mechanism.
 * Only one process (or one {@code LogDirectoryLock} within a process) may hold the lock at a time. A running node holds
 * the lock for its entire lifetime; CLI commands acquire it for the duration of the operation.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public final class LogDirectoryLock implements Closeable {

    private static final String LOCK_FILE_NAME = "raft.lock";

    private final Path lockPath;
    private FileChannel channel;
    private FileLock lock;

    /**
     * Creates a lock handle for the given log directory. The lock is not acquired until {@link #tryAcquire()} is called.
     *
     * @param logDir the Raft log directory containing (or that will contain) the lock file
     */
    public LogDirectoryLock(File logDir) {
        this.lockPath = logDir.toPath().resolve(LOCK_FILE_NAME);
    }

    /**
     * Attempts to acquire an exclusive lock on the log directory.
     *
     * <p>
     * If the lock is held by another operating system process, this method returns {@code false} without blocking. If the
     * lock is already held by this JVM (same process, different call site), an {@link IOException} is thrown with a
     * descriptive message.
     * </p>
     *
     * @return {@code true} if the lock was acquired, {@code false} if another process holds it
     * @throws IOException if the lock file cannot be opened, or if this JVM already holds the lock
     */
    public boolean tryAcquire() throws IOException {
        channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            channel.close();
            channel = null;
            String message = String.format("Log directory %s is already locked by the current JVM. " +
                    "Check whether you have multiple RAFT instances pointing to the same directory", lockPath.getParent());
            throw new IOException(message, e);
        }

        if (lock == null) {
            channel.close();
            channel = null;
            return false;
        }

        return lock.isValid();
    }

    /**
     * Releases the lock and closes the underlying file channel.
     *
     * <p>
     * Safe to call multiple times or without a prior {@link #tryAcquire()} call.
     * </p>
     *
     * @throws IOException if releasing the lock or closing the channel fails
     */
    @Override
    public void close() throws IOException {
        if (lock != null) {
            lock.release();
            lock = null;
        }

        // Observe that we do NOT delete the lock file, it still remains but unlocked.
        // The file could be acquired by another process and the delete operation could cause a concurrency issue.
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }
}
