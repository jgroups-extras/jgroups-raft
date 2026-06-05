package org.jgroups.raft.cli.commands.log.repair;

import org.jgroups.raft.filelog.MetadataStorage;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import picocli.CommandLine;

/**
 * Reconstructs the metadata file from log entries when the original metadata is truncated or unreadable.
 *
 * <p>
 * Writes a new metadata file with a conservative commit index of zero and the current term set to the highest term found
 * in the log entries. The voted-for field is cleared. On restart, the state machine re-applies entries from the beginning
 * of the log, so it must handle duplicate applications safely.
 * </p>
 *
 * @param highestEntryTerm the highest term found across all log entries, or {@code 0} if the log is empty
 */
record ReconstructMetadata(long highestEntryTerm) implements RepairAction {

    @Override
    public void describe(PrintWriter out, CommandLine.Help.Ansi ansi) {
        out.println("Repair action:");
        out.println("  Reconstruct metadata from the log:");
        out.printf("    Commit index:  0 (conservative — the leader will re-commit entries)%n");
        out.printf("    Current term:  %d (highest term found in log entries)%n", highestEntryTerm);
        out.println("    Voted for:     cleared");
        out.println();
        out.println(ansi.string("@|bold Warnings:|@"));
        out.println(ansi.string(
                "  @|yellow Setting commit index to 0 means the state machine will|@"));
        out.println(ansi.string(
                "  @|yellow re-apply entries from the beginning of the log on restart.|@"));
        out.println(ansi.string(
                "  @|yellow The state machine must handle duplicate applications safely.|@"));
        out.println(ansi.string(
                "  @|yellow Clearing the vote record means this node may vote again in|@"));
        out.println(ansi.string(String.format(
                "  @|yellow term %d if an election is already in progress. In rare cases,|@", highestEntryTerm)));
        out.println(ansi.string(
                "  @|yellow this could allow two leaders in the same term. This risk is|@"));
        out.println(ansi.string(
                "  @|yellow minimal if the cluster is healthy and only this node is recovering.|@"));
    }

    @Override
    public String confirmationPrompt() {
        return "Proceed with metadata reconstruction?";
    }

    @Override
    public void execute(Path directory) throws IOException {
        Path metadataPath = directory.resolve(MetadataStorage.FILE_NAME);
        try (FileChannel ch = FileChannel.open(metadataPath, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
            buf.putLong(0L);
            buf.putLong(highestEntryTerm);
            buf.flip();

            int written = ch.write(buf, 0);
            if (written != buf.capacity()) {
                String message = String.format("Metadata write incomplete: expected %d bytes, wrote %d",
                        buf.capacity(), written);
                throw new IOException(message);
            }
        }
    }

    @Override
    public void describeCompletion(PrintWriter out) {
        out.printf("Metadata reconstructed. Commit index: 0, current term: %d.%n", highestEntryTerm);
    }
}
