package org.jgroups.raft.cli.commands;

import org.jgroups.raft.cli.commands.log.LogValidation;
import org.jgroups.raft.cli.commands.log.LogValidationOptions;
import org.jgroups.raft.cli.commands.log.ValidationResult;
import org.jgroups.raft.cli.commands.log.repair.RepairAction;
import org.jgroups.raft.filelog.LogEntryStorage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

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
 * @since 2.0
 * @author José Bolina
 */
@Command(name = "repair", description = "Fix recoverable corruption in the log directory.")
final class LogRepair extends BaseLogCommand {

    /**
     * Runs a full verification pass and executes identified repair actions after operator confirmation.
     *
     * <p>
     * Returns immediately without modifying any file when no corruption is found, the format
     * is unrecognized (delegates to header reconstruction), or no manageable repair actions
     * are identified.
     * </p>
     *
     * @return {@link #EXIT_OK} if no corruption is found or all repairs complete successfully,
     *         {@link #EXIT_CORRUPTION} if the operator declines a repair action,
     *         {@link #EXIT_INVALID} if the log format prevents repair
     * @throws IOException if an I/O error prevents verification or repair
     */
    @Override
    protected int execute() throws IOException {
        // The scan results ignores the verbosity level when scanning for repair.
        // The repair won't show the information from log verify.
        ValidationResult result = LogValidation.validate(directory(), LogValidationOptions.simple());

        if (result.isValid()) {
            result.logInfo().ifPresent(info ->
                    out().printf("  Entries:   %d - %d (%d entries)%n",
                            info.firstIndex(), info.lastIndex(), info.entryCount()));
            out().println("  All checksums OK.");
            out().println();
            out().println("  No further repair needed.");
            out().println();
            out().println("Restart the node normally.");
            return EXIT_OK;
        }

        // An unrecognized file means we were not even able to parse the file header.
        // We reconstruct the header and parse the file.
        if (result.fileParsed() == ValidationResult.ParseType.UNRECOGNIZED)
            return handleHeaderReconstruction();

        // Otherwise, there was an issue in the file content.
        return repairFromValidation(result);
    }

    /**
     * Maps the validation result to repair actions and executes each one in order.
     *
     * @param result  the validation result with at least one violation
     * @return {@link #EXIT_OK} if repair completes, {@link #EXIT_CORRUPTION} if the operator declines or no
     *         manageable corruption point is found, {@link #EXIT_INVALID} if the format prevents repair
     * @throws IOException if file operations fail
     */
    private int repairFromValidation(ValidationResult result) throws IOException {
        List<RepairAction> actions = RepairAction.identify(result);

        if (actions.isEmpty()) {
            out().println("Not identified a manageable repair actions to fix the issues.");
            return result.exitCode();
        }

        CommandLine.Help.Ansi ansi = spec().commandLine().getColorScheme().ansi();
        Path directory = directory().toPath();

        for (RepairAction action : actions) {
            action.describe(out(), ansi);
            out().println();

            if (!fileOperationConfirmation(action.confirmationPrompt()))
                return EXIT_CORRUPTION;

            action.execute(directory);
            out().println();
            action.describeCompletion(out());
        }

        out().println();
        out().println("Restart the node to begin recovery.");
        return EXIT_OK;
    }

    /**
     * Handles the two-phase header reconstruction flow.
     *
     * @return the exit code
     * @throws IOException if file operations fail
     */
    private int handleHeaderReconstruction() throws IOException {
        CommandLine.Help.Ansi ansi = spec().commandLine().getColorScheme().ansi();

        out().println("Repair action:");
        out().println("  Reconstruct the file header.");
        out().println("  The header contains only the format identifier and version.");
        out().println("  No entry data is affected.");
        out().println();
        out().println("  After header reconstruction, entries will be scanned for");
        out().println("  additional corruption.");
        out().println();
        out().println(ansi.string("@|bold Warnings:|@"));
        out().println(ansi.string("  @|yellow The original header bytes are overwritten. If the file is|@"));
        out().println(ansi.string("  @|yellow not actually a v2 log file, this operation will not help.|@"));
        out().println();

        if (!fileOperationConfirmation("Proceed with header reconstruction?"))
            return EXIT_CORRUPTION;

        reconstructHeader();
        out().println();
        out().println("Header reconstructed. Scanning entries...");
        out().println();

        // After the file header is rectified, submit again for execution.
        // This should allow the full file to be parsed for issues.
        return execute();
    }

    /**
     * Overwrites the first 8 bytes of the entries file with a valid v2 file header.
     *
     * @throws IOException if the entries file cannot be opened or the write is incomplete
     */
    public void reconstructHeader() throws IOException {
        Path entriesPath = directory().toPath().resolve(LogEntryStorage.FILE_NAME);
        try (FileChannel ch = FileChannel.open(entriesPath, StandardOpenOption.WRITE)) {
            ByteBuffer header = ByteBuffer.allocate(LogEntryStorage.FILE_HEADER_SIZE);
            LogEntryStorage.writeFileHeaderTo(header);
            header.flip();

            int written = ch.write(header, 0);
            if (written != LogEntryStorage.FILE_HEADER_SIZE) {
                String message = String.format("Header write incomplete: expected %d bytes, wrote %d",
                        LogEntryStorage.FILE_HEADER_SIZE, written);
                throw new IOException(message);
            }
        }
    }
}
