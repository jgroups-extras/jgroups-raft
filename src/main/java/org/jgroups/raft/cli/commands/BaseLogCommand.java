package org.jgroups.raft.cli.commands;

import org.jgroups.raft.filelog.LogDirectoryLock;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    private boolean doesUserConfirm() {
        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8);
        if (scanner.hasNextLine()) {
            String input = scanner.nextLine().trim();
            return PROMPT_YES.equals(input);
        }
        return false;
    }
}
