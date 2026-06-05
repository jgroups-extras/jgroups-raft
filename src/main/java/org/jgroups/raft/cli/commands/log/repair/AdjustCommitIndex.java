package org.jgroups.raft.cli.commands.log.repair;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

import picocli.CommandLine;

/**
 * Adjusts the metadata commit index down to the last log entry.
 *
 * <p>
 * Created as a standalone action only when entries are intact but the metadata commit index exceeds the log range. When
 * entry corruption is also present, {@link TruncateEntries} handles the commit index adjustment internally.
 * </p>
 *
 * @param current the current commit index from metadata
 * @param target  the corrected commit index (last log entry index)
 */
record AdjustCommitIndex(long current, long target) implements RepairAction {

    @Override
    public void describe(PrintWriter out, CommandLine.Help.Ansi ansi) {
        out.println("Repair action:");
        out.printf("  Adjust commit index from %d to %d (last log entry).%n", current, target);
        out.println();

        long missing = current - target;
        out.printf("  The %d missing committed entries cannot be recovered from%n", missing);
        out.println("  this node's files. The leader will re-send them after restart.");
    }

    @Override
    public String confirmationPrompt() {
        return "Proceed with commit index adjustment?";
    }

    @Override
    public void execute(Path directory) throws IOException {
        EntryFileRepairUtil.adjustCommitIndex(directory, target);
    }

    @Override
    public void describeCompletion(PrintWriter out) {
        out.printf("Commit index adjusted to %d.%n", target);
    }
}
