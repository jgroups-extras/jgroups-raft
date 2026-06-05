package org.jgroups.raft.cli.commands.log.repair;

import org.jgroups.raft.cli.commands.log.ValidationResult;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

import picocli.CommandLine;

/**
 * Truncates the entries file at the first corruption point.
 *
 * <p>
 * Removes the corrupted entry and all entries after it, even if later entries have valid checksums,
 * because the Raft log cannot have gaps. If the metadata commit index exceeds the truncation point,
 * it is adjusted downward to match the last intact entry.
 * </p>
 *
 * @param corruption         the first corruption point in the entries file
 * @param logInfo            the log entry range from the scan, or {@code null} if no entries were scanned
 * @param lastIntactIndex    the last intact entry index, or {@code -1} if no intact entries exist
 * @param metadataCommitIndex the commit index from metadata, or {@code -1} if metadata is not readable
 * @param adjustCommit       whether the metadata commit index needs to be lowered
 */
record TruncateEntries(
        ValidationResult.CorruptionPoint corruption,
        ValidationResult.LogInfo logInfo,
        long lastIntactIndex,
        long metadataCommitIndex,
        boolean adjustCommit
) implements RepairAction {

    @Override
    public void describe(PrintWriter out, CommandLine.Help.Ansi ansi) {
        out.println("Repair action:");

        if (isCrashRecovery()) {
            out.printf("  Remove incomplete trailing bytes at offset %d.%n", corruption.offset());
            if (logInfo != null && logInfo.entryCount() > 0) {
                out.printf("  All %d complete entries (%d - %d) are preserved.%n",
                        logInfo.entryCount(), logInfo.firstIndex(), lastIntactIndex);
            }
        } else if (lastIntactIndex > 0) {
            out.printf("  Truncate log to entry %d.%n", lastIntactIndex);
            if (logInfo != null && corruption.index() > 0) {
                long entriesToRemove = logInfo.lastIndex() - lastIntactIndex;
                out.printf("  %d entries will be permanently removed (%d - %d).%n",
                        entriesToRemove, corruption.index(), logInfo.lastIndex());
            }
        } else {
            out.printf("  Truncate log at offset %d (no intact entries remain).%n", corruption.offset());
        }

        if (adjustCommit) {
            out.printf("  Commit index will be adjusted from %d to %d.%n", metadataCommitIndex, lastIntactIndex);
        }

        out.println();
        out.println("After repair:");
        out.println("  Restart the node. It will rejoin the cluster and recover missing entries from the current " +
                "leader automatically.");
        out.println();

        out.println(ansi.string("@|bold Warnings:|@"));

        if (isCrashRecovery()) {
            out.println(ansi.string("  @|yellow The incomplete entry was never committed. No data is lost.|@"));
            return;
        }

        if (corruption.type() == ValidationResult.CorruptionPoint.Type.CRC_MISMATCH
                && logInfo != null && corruption.index() > 0) {
            long entriesAfter = logInfo.lastIndex() - corruption.index();
            if (entriesAfter > 0) {
                String message = String.format("  @|yellow %d entries after the first corrupted entry will be removed. " +
                        "The log cannot have gaps. The leader should re-send them after restart.|@", entriesAfter);
                out.println(ansi.string(message));
            }
        }

        if (adjustCommit) {
            if (corruption.index() > 0) {
                String message = String.format("  @|yellow Entries %d - %d were previously committed. " +
                                "The leader should re-send them after the node joins the cluster.|@",
                        corruption.index(), metadataCommitIndex);
                out.println(ansi.string(message));
            } else {
                String message = String.format("  @|yellow Commit index will be lowered from %d to %d. Previously " +
                                "committed entries beyond this point will need to be re-transmitted by the leader.|@",
                        metadataCommitIndex, lastIntactIndex);
                out.println(ansi.string(message));
            }
        }
    }

    @Override
    public String confirmationPrompt() {
        return "Proceed with described repair?";
    }

    @Override
    public void execute(Path directory) throws IOException {
        EntryFileRepairUtil.truncateEntriesFile(directory, corruption.offset());
        if (adjustCommit)
            EntryFileRepairUtil.adjustCommitIndex(directory, lastIntactIndex);
    }

    @Override
    public void describeCompletion(PrintWriter out) {
        if (lastIntactIndex > 0) {
            out.printf("Log truncated to entry %d.%n", lastIntactIndex);
        } else {
            out.println("Log truncated. No entries remain.");
        }

        if (adjustCommit) {
            out.printf("Commit index adjusted to %d.%n", lastIntactIndex);
        }
    }

    private boolean isCrashRecovery() {
        return corruption.type() == ValidationResult.CorruptionPoint.Type.INCOMPLETE_ENTRY
                || corruption.type() == ValidationResult.CorruptionPoint.Type.TRUNCATED_HEADER;
    }
}
