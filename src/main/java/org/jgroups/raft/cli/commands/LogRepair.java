package org.jgroups.raft.cli.commands;

import org.jgroups.raft.cli.commands.log.LogValidation;
import org.jgroups.raft.cli.commands.log.LogValidationOptions;
import org.jgroups.raft.cli.commands.log.ValidationResult;
import org.jgroups.raft.filelog.LogEntryStorage;
import org.jgroups.raft.filelog.MetadataStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Repairs recoverable corruption in the entries file of a Raft log directory.
 *
 * <p>
 * Runs a full verification pass to identify corruption, presents the findings and proposed repair actions to the
 * operator, and truncates the entries file at the first corruption point after explicit double confirmation. If the
 * metadata commit index exceeds the truncation point, it is adjusted downward to match the last intact entry.
 * </p>
 *
 * <p>
 * The repair always truncates at the first corruption point. Entries after the corruption are removed even if they
 * have valid checksums, because the Raft log cannot have gaps. Two confirmation prompts are required before any file
 * is modified: a backup acknowledgement and an action-specific confirmation.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@Command(name = "repair", description = "Fix recoverable corruption in the log directory.")
final class LogRepair extends BaseLogCommand {

    /**
     * Runs a full verification pass, presents the repair plan to the operator, and truncates the entries file
     * at the first corruption point after double confirmation.
     *
     * <p>
     * The method returns immediately without modifying any file when:
     * <ul>
     *   <li>No corruption is found, the log is healthy.</li>
     *   <li>The validation result indicates an invalid format (v1, unrecognized): entry repair is not applicable.</li>
     *   <li>No entry-level corruption is found: non-entry issues (snapshot, metadata) are handled by dedicated repair tasks.</li>
     *   <li>The operator declines either confirmation prompt.</li>
     * </ul>
     * </p>
     *
     * @return {@link #EXIT_OK} if no corruption is found or the repair completes successfully,
     *         {@link #EXIT_CORRUPTION} if corruption is found and the operator declines repair,
     *         {@link #EXIT_INVALID} if the log format prevents repair
     * @throws IOException if an I/O error prevents the verification or repair
     */
    @Override
    protected int execute() throws IOException {
        // The scan results ignores the verbosity level when scanning for repair.
        ValidationResult result = LogValidation.validate(directory(), LogValidationOptions.simple());

        if (result.isValid()) {
            out().println("No corruption found. Nothing to repair");
            return EXIT_OK;
        }

        Optional<ValidationResult.CorruptionPoint> corruption = result.firstCorruption();
        if (corruption.isEmpty()) {
            out().println("Not identified a manageable corruption point.");
            return result.exitCode();
        }

        ValidationResult.CorruptionPoint cp = corruption.get();
        ValidationResult.LogInfo li = result.logInfo().orElse(null);
        ValidationResult.MetadataInfo mi = result.metadataInfo().orElse(null);

        long lastIntactIndex = resolveLastIntactIndex(cp, li);
        boolean adjustCommit = mi != null && lastIntactIndex >= 0 && mi.commitIndex() > lastIntactIndex;

        describeRepairAction(cp, li, lastIntactIndex);

        if (adjustCommit) {
            out().printf("  Commit index will be adjusted from %d to %d.%n", mi.commitIndex(), lastIntactIndex);
        }

        out().println();
        describeAfterRepair();
        out().println();
        describeWarnings(cp, li, lastIntactIndex, adjustCommit, mi);

        if (!fileOperationConfirmation("Proceed with repair?"))
            return EXIT_CORRUPTION;

        truncateEntriesFile(cp.offset());
        if (adjustCommit) {
            adjustCommitIndex(lastIntactIndex);
        }

        out().println();
        describeCompletion(lastIntactIndex, adjustCommit);
        return EXIT_OK;
    }

    /**
     * Determines the Raft log index of the last intact entry before the corruption point.
     *
     * <p>
     * When the entry header at the corruption point is readable ({@link ValidationResult.CorruptionPoint.Type#CRC_MISMATCH} or
     * {@link ValidationResult.CorruptionPoint.Type#INCOMPLETE_ENTRY}), the index is derived directly from the corruption point.
     * When the header is unreadable ({@link ValidationResult.CorruptionPoint.Type#TRUNCATED_HEADER},
     * {@link ValidationResult.CorruptionPoint.Type#INVALID_MAGIC}, {@link ValidationResult.CorruptionPoint.Type#INVALID_HEADER}),
     * the last successfully scanned entry from the log info is used instead.
     * </p>
     *
     * @param cp      the corruption point
     * @param logInfo the log entry range from the scan, or {@code null} if no entries were scanned
     * @return the last intact entry index, or {@code -1} if no intact entries exist
     */
    private long resolveLastIntactIndex(ValidationResult.CorruptionPoint cp, ValidationResult.LogInfo logInfo) {
        if (cp.index() > 0)
            return cp.index() - 1;

        if (logInfo != null)
            return logInfo.lastIndex();

        return -1;
    }

    /**
     * Prints the repair action description, tailored to the corruption type.
     *
     * @param cp              the corruption point
     * @param logInfo         the log entry range, or {@code null} if no entries were scanned
     * @param lastIntactIndex the last intact entry index
     */
    private void describeRepairAction(ValidationResult.CorruptionPoint cp, ValidationResult.LogInfo logInfo, long lastIntactIndex) {
        out().println("Repair action:");

        if (isCrashRecovery(cp.type())) {
            out().printf("  Remove incomplete trailing bytes at offset %d.%n", cp.offset());
            if (logInfo != null && logInfo.entryCount() > 0) {
                out().printf("  All %d complete entries (%d - %d) are preserved.%n",
                        logInfo.entryCount(), logInfo.firstIndex(), lastIntactIndex);
            }
            return;
        }

        if (lastIntactIndex > 0) {
            out().printf("  Truncate log to entry %d.%n", lastIntactIndex);
        } else {
            out().printf("  Truncate log at offset %d (no intact entries remain).%n", cp.offset());
        }

        if (logInfo != null && cp.index() > 0) {
            long entriesToRemove = logInfo.lastIndex() - lastIntactIndex;
            out().printf("  %d entries will be permanently removed (%d - %d).%n",
                    entriesToRemove, cp.index(), logInfo.lastIndex());
        }
    }

    /**
     * Prints the post-repair instructions for the operator.
     */
    private void describeAfterRepair() {
        out().println("After repair:");
        out().println("  Restart the node. It will rejoin the cluster and recover missing entries from the current " +
                "leader automatically.");
    }

    /**
     * Prints the warnings block with yellow highlighting, tailored to the corruption type.
     *
     * @param cp              the corruption point
     * @param logInfo         the log entry range, or {@code null} if no entries were scanned
     * @param lastIntactIndex the last intact entry index
     * @param adjustCommit    whether the commit index will be adjusted downward
     * @param metaInfo        the metadata values, or {@code null} if metadata is unreadable
     */
    private void describeWarnings(ValidationResult.CorruptionPoint cp, ValidationResult.LogInfo logInfo, long lastIntactIndex,
                                  boolean adjustCommit, ValidationResult.MetadataInfo metaInfo) {
        CommandLine.Help.Ansi ansi = spec().commandLine().getColorScheme().ansi();
        out().println(ansi.string("@|bold Warnings:|@"));

        // A failure because the last entry in the lost is incomplete.
        // This could happen if the system suddenly crashes before finished writing.
        // Everything before the truncated entry is good and safe.
        if (isCrashRecovery(cp.type())) {
            out().println(ansi.string("  @|yellow The incomplete entry was never committed. No data is lost.|@"));
            return;
        }

        // An entry in the log has a CRC mismatch.
        // The data was corrupted after the write to disk.
        // This repair operation involves truncating the file to contain up-to the last good entry.
        // The removed data should be replicated by the leader again.
        if (cp.type() == ValidationResult.CorruptionPoint.Type.CRC_MISMATCH && logInfo != null && cp.index() > 0) {
            long entriesAfter = logInfo.lastIndex() - cp.index();
            if (entriesAfter > 0) {
                String message = String.format("  @|yellow %d entries after the first corrupted entry will be removed. " +
                        "The log cannot have gaps. The leader should re-send them after restart.|@", entriesAfter);
                out().println(ansi.string(message));
            }
        }

        if (adjustCommit && metaInfo != null) {
            if (cp.index() > 0) {
                String message = String.format("  @|yellow Entries %d - %d were previously committed. " +
                                "The leader should re-send them after the node joins the cluster.|@",
                        cp.index(), metaInfo.commitIndex());
                out().println(ansi.string(message));
            } else {
                String message = String.format("  @|yellow Commit index will be lowered from %d to %d. Previously " +
                                "committed entries beyond this point will need to be re-transmitted by the leader.|@",
                        metaInfo.commitIndex(), lastIntactIndex);
                out().println(ansi.string(message));
            }
        }
    }

    /**
     * Truncates the entries file at the given offset, preserving the file header and all entries before it.
     *
     * @param offset the file offset at which to truncate
     * @throws IOException if the file cannot be opened or truncated
     */
    private void truncateEntriesFile(long offset) throws IOException {
        if (offset < LogEntryStorage.FILE_HEADER_SIZE) {
            String message = String.format("Refusing to truncate below file header size: offset %d < header size %d",
                    offset, LogEntryStorage.FILE_HEADER_SIZE);
            throw new IOException(message);
        }

        Path entriesPath = directory().toPath().resolve(LogEntryStorage.FILE_NAME);
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
     * @param newCommitIndex the adjusted commit index value
     * @throws IOException if the metadata file cannot be opened or written
     */
    private void adjustCommitIndex(long newCommitIndex) throws IOException {
        Path metadataPath = directory().toPath().resolve(MetadataStorage.FILE_NAME);
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
     * Prints a summary of the completed repair actions to the operator.
     *
     * @param lastIntactIndex the last preserved entry index after truncation
     * @param adjustedCommit  whether the commit index was adjusted
     */
    private void describeCompletion(long lastIntactIndex, boolean adjustedCommit) {
        if (lastIntactIndex > 0) {
            out().printf("Log truncated to entry %d.%n", lastIntactIndex);
        } else {
            out().println("Log truncated. No entries remain.");
        }

        if (adjustedCommit) {
            out().printf("Commit index adjusted to %d.%n", lastIntactIndex);
        }

        out().println();
        out().println("Restart the node to begin recovery.");
    }

    private boolean isCrashRecovery(ValidationResult.CorruptionPoint.Type type) {
        return type == ValidationResult.CorruptionPoint.Type.INCOMPLETE_ENTRY
                || type == ValidationResult.CorruptionPoint.Type.TRUNCATED_HEADER;
    }
}
