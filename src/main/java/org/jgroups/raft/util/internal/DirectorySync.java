package org.jgroups.raft.util.internal;

import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Makes file creations, renames, and deletions inside a directory durable.
 *
 * <p>
 * Forcing a file only persists its content. The directory entry pointing to the file is persisted separately, so after
 * replacing a file through a rename, a crash can still bring back the old file unless the directory is also synced.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public final class DirectorySync {

    private static final boolean IS_WINDOWS = Util.checkForWindows();

    private DirectorySync() { }

    /**
     * Persists the contents of the given directory.
     *
     * @param directory the directory holding the files that were created, renamed, or deleted
     * @throws IOException if the directory cannot be synced
     */
    public static void sync(Path directory) throws IOException {
        if (IS_WINDOWS)
            return;

        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}
