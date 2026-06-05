package org.jgroups.raft.cli.commands.log.repair;

import org.jgroups.raft.cli.commands.log.ValidationResult;
import org.jgroups.raft.filelog.LogEntryStorage;
import org.jgroups.raft.filelog.MetadataStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class EntryFileRepairUtil {

    private EntryFileRepairUtil() { }

    /**
     * Truncates the entries file at the given offset, preserving the file header and all entries before it.
     *
     * @param directory the directory containing the data files
     * @param offset the file offset at which to truncate
     * @throws IOException if the file cannot be opened or truncated
     */
    public static void truncateEntriesFile(Path directory, long offset) throws IOException {
        if (offset < LogEntryStorage.FILE_HEADER_SIZE) {
            String message = String.format("Refusing to truncate below file header size: offset %d < header size %d",
                    offset, LogEntryStorage.FILE_HEADER_SIZE);
            throw new IOException(message);
        }

        Path entriesPath = directory.resolve(LogEntryStorage.FILE_NAME);
        try (FileChannel ch = FileChannel.open(entriesPath, StandardOpenOption.WRITE)) {
            ch.truncate(offset);

            long actualSize = ch.size();
            if (actualSize != offset) {
                String message = String.format("Truncation verification failed: expected file size %d; actual %d",
                        offset, actualSize);
                throw new IOException(message);
            }
        }
    }

    /**
     * Overwrites the commit index in the metadata file with the given value.
     *
     * <p>
     * Writes an 8-byte long at offset 0, which is the commit index position in the metadata layout. The current term and
     * voted-for fields are left unchanged.
     * </p>
     *
     * @param directory the directory containing the data files
     * @param newCommitIndex the adjusted commit index value
     * @throws IOException if the metadata file cannot be opened or written
     */
    public static void adjustCommitIndex(Path directory, long newCommitIndex) throws IOException {
        Path metadataPath = directory.resolve(MetadataStorage.FILE_NAME);
        try (FileChannel ch = FileChannel.open(metadataPath, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(newCommitIndex);
            buf.flip();

            int written = ch.write(buf, 0);
            if (written != Long.BYTES) {
                String message = String.format("Commit index write incomplete: expected to write %d bytes, wrote %d",
                        Long.BYTES, written);
                throw new IOException(message);
            }
        }
    }

    /**
     * Determines the Raft log index of the last intact entry before the corruption point.
     *
     * <p>
     * When the entry header at the corruption point is readable, the index is derived directly.
     * When the header is unreadable, the last successfully scanned entry from the log info is used.
     * </p>
     *
     * @param cp      the corruption point
     * @param logInfo the log entry range from the scan, or {@code null} if no entries were scanned
     * @return the last intact entry index, or {@code -1} if no intact entries exist
     */
    public static long resolveLastIntactIndex(ValidationResult.CorruptionPoint cp, ValidationResult.LogInfo logInfo) {
        if (cp.index() > 0)
            return cp.index() - 1;
        if (logInfo != null)
            return logInfo.lastIndex();
        return -1;
    }
}
