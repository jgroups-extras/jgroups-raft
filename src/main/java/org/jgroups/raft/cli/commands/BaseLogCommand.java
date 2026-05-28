package org.jgroups.raft.cli.commands;

import org.jgroups.raft.cli.commands.log.EntryCallback;
import org.jgroups.raft.cli.commands.log.LogValidationOptions;
import org.jgroups.raft.filelog.LogDirectoryLock;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.Callable;

import picocli.CommandLine;
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

    // Lazy initialize the scanner.
    private Scanner promptScanner;

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
        if (promptScanner == null)
            promptScanner = new Scanner(System.in, StandardCharsets.UTF_8);

        if (promptScanner.hasNextLine()) {
            String input = promptScanner.nextLine().trim();
            return PROMPT_YES.equals(input);
        }
        return false;
    }

    protected final LogValidationOptions validationOptions() {
        CommandLine.Help.Ansi ansi = spec().commandLine().getColorScheme().ansi();
        return switch (verbosityLevel()) {
            case 1 -> LogValidationOptions.withCallback(new VerboseCallback(out(), ansi));
            case 2 -> LogValidationOptions.withCallback(new HexDumpCallback(out(), ansi));
            default -> LogValidationOptions.simple();
        };
    }

    /**
     * Formats one line per entry with parsed header fields and CRC status.
     *
     * <p>
     * Entry numbers are Raft log indices, not ordinal positions in the file. Legacy entries display {@code CRC N/A (legacy)}
     * since they have no checksum. CRC mismatches include a second line with the expected and actual checksum values.
     * </p>
     *
     * @author José Bolina
     * @since 2.0
     */
    private record VerboseCallback(PrintWriter out, CommandLine.Help.Ansi ansi) implements EntryCallback {
        @Override
        public void onEntry(long index, long term, boolean internal, int dataLength, byte magic, CrcStatus status, byte[] payload) {
            out.printf("Entry %-6d term=%-4d index=%-6d internal=%-3s data=%-6s %s%n",
                    index, term, index, internal ? "yes" : "no", Util.printBytes(dataLength), colorStatus(status));

            if (status instanceof CrcStatus.Mismatch m) {
                String err = String.format("  @|red expected: 0x%08X  actual: 0x%08X|@%n", m.stored(), m.computed());
                out.printf(ansi.string(err));
            }
        }

        private String colorStatus(CrcStatus status) {
            if (status instanceof CrcStatus.Ok)
                return ansi.string(String.format("@|green %s|@", status));

            if (status instanceof CrcStatus.Legacy)
                return ansi.string(String.format("@|yellow %s|@", status));

            if (status instanceof CrcStatus.Mismatch)
                return ansi.string(String.format("@|bold,red *** %s ***|@", status));

            return status.toString();
        }
    }

    /**
     * Formats each entry with parsed header fields, CRC status, and a Wireshark-style hex dump of the payload.
     *
     * <p>
     * Delegates the header line to {@link VerboseCallback} and appends a hex dump with 16 bytes per line, grouped in two
     * 8-byte halves, with an ASCII sidebar showing printable characters.
     * </p>
     *
     * @since 2.0
     * @author José Bolina
     */
    private static final class HexDumpCallback implements EntryCallback {
        private final VerboseCallback delegate;
        private final PrintWriter out;

        HexDumpCallback(PrintWriter out, CommandLine.Help.Ansi ansi) {
            this.delegate = new VerboseCallback(out, ansi);
            this.out = out;
        }

        @Override
        public void onEntry(long index, long term, boolean internal, int dataLength, byte magic, CrcStatus status, byte[] payload) {
            delegate.onEntry(index, term, internal, dataLength, magic, status, payload);

            if (payload.length > 0) {
                dump(payload);
            }
        }

        /**
         * Writes a hex dump of the given data in Wireshark-style format.
         *
         * <p>
         * Each line shows a file offset, 16 hex bytes grouped in two 8-byte halves, and an ASCII sidebar where
         * non-printable bytes are replaced with {@code '.'}.
         * </p>
         *
         * @param data the raw bytes to dump
         */
        private void dump(byte[] data) {
            for (int offset = 0; offset < data.length; offset += 16) {
                int lineLen = Math.min(16, data.length - offset);
                StringBuilder hex = new StringBuilder();
                StringBuilder ascii = new StringBuilder();

                for (int i = 0; i < 16; i++) {
                    if (i == 8) hex.append(' ');
                    if (i < lineLen) {
                        byte b = data[offset + i];
                        hex.append(String.format("%02x ", b));
                        ascii.append(b >= 0x20 && b < 0x7F ? (char) b : '.');
                    } else {
                        hex.append("   ");
                    }
                }

                out.printf("   %08x  %s |%s|%n", offset, hex, ascii);
            }
            out.println();
        }
    }
}
