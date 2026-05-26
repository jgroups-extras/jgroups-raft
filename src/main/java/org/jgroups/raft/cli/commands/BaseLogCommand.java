package org.jgroups.raft.cli.commands;

import org.jgroups.raft.filelog.LogDirectoryLock;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;
import java.util.concurrent.Callable;

import picocli.CommandLine.Parameters;

/**
 * Abstract base for offline log commands.
 *
 * <p>
 * Enforces a shared pre-flight sequence before any subcommand logic runs: validate the directory, acquire an exclusive
 * {@link LogDirectoryLock}, and reject v1-format log files. Subcommands implement {@link #execute()} with the guarantee
 * that these checks have already passed.
 * </p>
 *
 * <p>
 * This class implements {@link Callable} to return an exit code directly to PicoCLI. The three exit codes are defined as
 * constants: {@link #EXIT_OK}, {@link #EXIT_CORRUPTION}, and {@link #EXIT_INVALID}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
abstract class BaseLogCommand extends BaseRaftCLICommand implements Callable<Integer> {
    // Exit code for clean and successful operation.
    static final int EXIT_OK = 0;

    // Exit code when corruption, inconsistency is detected, or the operation fails.
    static final int EXIT_CORRUPTION = 1;

    // Exist code for invalid arguments, locked directory, invalid file format, or missing directory.
    static final int EXIT_INVALID = 2;

    // The magic word to confirm the prompt before doing dangerous work.
    static final String PROMPT_YES = "yes";

    // Constant values defined in the Log implementation.
    private static final byte[] RAFT_MAGIC = {'R', 'A', 'F', 'T'};
    private static final byte LEGACY_MAGIC = 0x01;
    private static final String ENTRIES_FILE = "entries.raft";

    @Parameters(index = "0", description = "Path to the Raft log directory")
    private File logDir;

    private boolean backupConfirmed;

    @Override
    public final void run() { }

    /**
     * Subcommand-specific logic, invoked after all pre-flight checks have passed.
     *
     * @return the exit code
     * @throws IOException if an I/O error occurs during the operation
     */
    protected abstract int execute() throws IOException;

    /**
     * Orchestrates the pre-flight checks and delegates to the subcommand.
     *
     * <p>
     * Execution order:
     * <ol>
     *   <li>Validate that the path is an existing directory.</li>
     *   <li>Acquire an exclusive lock on the log directory.</li>
     *   <li>If {@code entries.raft} exists, reject v1 format.</li>
     *   <li>Delegate to {@link #execute()}.</li>
     * </ol>
     * </p>
     *
     * @return the exit code: {@link #EXIT_OK}, {@link #EXIT_CORRUPTION}, or {@link #EXIT_INVALID}
     */
    @Override
    public Integer call() {
        if (!directory().exists() || !directory().isDirectory()) {
            err().printf("Error: %s is not a valid directory.%n", directory().getAbsolutePath());
            return EXIT_INVALID;
        }

        try (LogDirectoryLock lock = new LogDirectoryLock(directory())) {
            if (!lock.tryAcquire()) {
                String message = String.format("Error: Unable to acquire lock for directory %s. " +
                        "Ensure no other process is currently running and pointing to that data directory.", directory().getAbsolutePath());
                err().println(message);
                return EXIT_INVALID;
            }

            int exit = isValidFile();
            if (exit >= 0)
                return exit;

            return execute();
        } catch (IOException e) {
            err().printf("Error: %s%n", e.getMessage());
            return EXIT_CORRUPTION;
        }
    }

    /**
     * Prompts the operator for a two-step confirmation: backup acknowledgement followed by an action-specific prompt.
     * Both default to NO.
     *
     * <p>
     * The backup prompt is asked at most once per invocation. Multi-phase repairs (e.g., header reconstruction followed
     * by truncation) share the same backup acknowledgement.
     * </p>
     *
     * @param prompt the action-specific question (e.g., "Proceed with repair?")
     * @return {@code true} only if the operator answers "yes" to both prompts
     */
    protected final boolean fileOperationConfirmation(String prompt) {
        if (!backupConfirmed) {
            out().printf("Have you backed up the log directory at %s? (yes/NO): ", directory().getAbsolutePath());
            out().flush();
            if (!doesUserConfirm())
                return false;

            backupConfirmed = true;
        }

        out().printf("%s (yes/NO): ", prompt);
        out().flush();
        return doesUserConfirm();
    }

    protected final File directory() {
        return logDir;
    }

    /**
     * Checks whether {@code entries.raft} is in v1 format (no file header, first byte is the legacy {@code 0x01} magic).
     * If so, prints guidance and returns {@link #EXIT_INVALID}.
     *
     * @return the exit code if v1 is detected, or {@code -1} if the check passed
     * @throws IOException if the file cannot be read
     */
    private int isValidFile() throws IOException {
        Path entriesPath = directory().toPath().resolve(ENTRIES_FILE);
        File entriesFile = entriesPath.toFile();
        if (!entriesFile.isFile() || entriesFile.length() == 0) {
            return -1;
        }

        try (FileChannel ch = FileChannel.open(entriesPath, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            int read = ch.read(buf);

            // Very likely an empty file.
            if (read < 4) {
                return -1;
            }
            buf.flip();

            // First verify if it is a legacy file.
            // Legacy files do not include a checksum for verification, so there is nothing to do here.
            if (buf.get(0) == LEGACY_MAGIC) {
                out().println(entriesPath.toAbsolutePath());
                out().println("  Format:    v1 (no checksums)");
                out().println();
                out().println("  This log is in v1 format. The CLI tool requires v2 format");
                out().println("  with CRC checksums for integrity verification.");
                out().println();
                out().println("  Start the node with a 2.x release to upgrade the log format");
                out().println("  automatically. New entries will be written with checksums.");
                return EXIT_INVALID;
            }

            // If the file magic matches the RAFT magic, we can proceed with the validation.
            if (buf.get(0) == RAFT_MAGIC[0] && buf.get(1) == RAFT_MAGIC[1]
                    && buf.get(2) == RAFT_MAGIC[2] && buf.get(3) == RAFT_MAGIC[3])
                return -1;

            // There is file with content and it is neither legacy nor the new format.
            // Let's assume it is a corrupted file already instead of doing work.
            return EXIT_CORRUPTION;
        }
    }

    private boolean doesUserConfirm() {
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8);
        if (scanner.hasNextLine()) {
            String input = scanner.nextLine().trim();
            return PROMPT_YES.equals(input);
        }
        return false;
    }
}
