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
 * Adjusts the metadata current term upward to match the highest term in the log entries.
 *
 * <p>
 * Only created when the metadata current term is lower than the highest entry term. The opposite
 * case (term higher than all entries) is not corruption — a node can legitimately have a higher
 * term from vote requests without appending entries in that term.
 * </p>
 *
 * @param current the current term from metadata
 * @param target  the corrected term (highest term found in log entries)
 */
record AdjustCurrentTerm(long current, long target) implements RepairAction {
    @Override
    public void describe(PrintWriter out, CommandLine.Help.Ansi ansi) {
        out.println("Repair action:");
        out.printf("  Adjust current term from %d to %d (highest term in log entries).%n", current, target);
        out.println();
        out.println(ansi.string("@|bold Warnings:|@"));
        out.println(ansi.string(
                "  @|yellow Increasing the term is safe — the node will not vote in a|@"));
        out.println(ansi.string(
                "  @|yellow stale term. However, if other nodes are in a lower term, this|@"));
        out.println(ansi.string(
                "  @|yellow node will reject their messages until they catch up.|@"));
    }

    @Override
    public String confirmationPrompt() {
        return "Proceed with term adjustment?";
    }

    @Override
    public void execute(Path directory) throws IOException {
        Path metadataPath = directory.resolve(MetadataStorage.FILE_NAME);
        try (FileChannel ch = FileChannel.open(metadataPath, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(target);
            buf.flip();

            int written = ch.write(buf, Long.BYTES);
            if (written != Long.BYTES) {
                String message = String.format("Current term write incomplete: expected %d bytes, wrote %d",
                        Long.BYTES, written);
                throw new IOException(message);
            }
        }
    }

    @Override
    public void describeCompletion(PrintWriter out) {
        out.printf("Current term adjusted to %d.%n", target);
    }
}
