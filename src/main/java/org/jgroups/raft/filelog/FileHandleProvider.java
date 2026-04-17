package org.jgroups.raft.filelog;

import org.jgroups.raft.util.pmem.FileProvider;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Provides a {@link FileChannel} and optional {@link RandomAccessFile} for a given file.
 *
 * <p>
 * The interface abstracts how files are created and provided during runtime.
 * </p>
 *
 * @since 2.0
 */
@FunctionalInterface
interface FileHandleProvider {

    /**
     * A file handle pairing a {@link FileChannel} with an optional {@link RandomAccessFile}.
     *
     * @param channel the channel for read/write/force operations
     * @param raf the underlying file, or {@code null} when backed by PMEM
     */
    record Handle(FileChannel channel, RandomAccessFile raf) { }

    /**
     * Opens the given file and returns a handle for I/O operations.
     *
     * @param file the file to open or create
     * @return a handle containing the channel and optional random access file
     * @throws IOException if the file cannot be opened
     */
    Handle open(File file) throws IOException;

    /**
     * Returns the default provider, which uses PMEM when available and falls back to {@link RandomAccessFile} otherwise.

     * @return the default provider
     */
    static FileHandleProvider defaultProvider() {
        return file -> {
            FileChannel pmemChannel = FileProvider.openPMEMChannel(file, 1024, true, true);
            if (pmemChannel == null) {
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                return new Handle(raf.getChannel(), raf);
            }
            return new Handle(pmemChannel, null);
        };
    }
}
