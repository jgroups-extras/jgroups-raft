package org.jgroups.raft.cli.exceptions;

import java.io.PrintWriter;

import picocli.CommandLine;

/**
 * Global exception handler for the CLI execution lifecycle.
 *
 * <p>
 * This class intercepts uncaught exceptions thrown during command execution and presents them in a user-friendly format.
 * Instead of dumping a raw stack trace to the console by default, it prints a colored error message and a hint on how
 * to enable debug output.
 * </p>
 *
 * It also standardizes the process exit codes based on the type of exception encountered.
 *
 * <p>
 * <table border="1">
 * <caption>Exit Code Reference</caption>
 * <tr>
 * <th>Exit Code</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>{@link #DEFAULT_EXCEPTION_CODE}</td>
 * <td>Standard error code used for general unexpected runtime exceptions.</td>
 * </tr>
 * <tr>
 * <td>{@link #PROBE_EXCEPTION_CODE}</td>
 * <td>Specific error code indicating a failure in the JGroups Probe network protocol (e.g., timeout, unreachable).</td>
 * </tr>
 * <tr>
 * <td>{@link #INVALID_ARGUMENT_CODE}</td>
 * <td>Error code used when invalid arguments are provided to the command.</td>
 * </tr>
 * </table>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public final class JGroupsRaftExceptionHandler implements CommandLine.IExecutionExceptionHandler {

    /**
     * Standard exit code for general execution errors.
     */
    public static final int DEFAULT_EXCEPTION_CODE = 1;

    /**
     * Specific exit code for errors related to the JGroups Probe network protocol.
     */
    public static final int PROBE_EXCEPTION_CODE = 101;

    /**
     * Exit code for invalid arguments provided to the CLI.
     * For example, an invalid socket address.
     */
    public static final int INVALID_ARGUMENT_CODE = 102;

    /**
     * Handles an exception thrown during the execution of a command.
     *
     * <p>
     * The logic follows these steps:
     * <ol>
     *   <li>Determines the appropriate exit code by checking if the exception is a {@link JGroupsProbeException}.</li>
     *   <li>Prints a formatted, red error message to {@code stderr}.</li>
     *   <li>If the {@code --verbose} flag is present, prints the full stack trace.</li>
     *   <li>Otherwise, prints a hint advising the user on how to see the stack trace.</li>
     * </ol>
     * </p>
     *
     * @param ex              The exception that was thrown.
     * @param commandLine     The command line instance that encountered the error.
     * @param fullParseResult The result of parsing the command line arguments.
     * @return The exit code to be returned to the operating system.
     */
    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine, CommandLine.ParseResult fullParseResult) {
        int exitCode = DEFAULT_EXCEPTION_CODE;

        // Map specific exceptions to distinct exit codes for scriptability
        if (ex instanceof JGroupsProbeException jpe)
            exitCode = jpe.exitCode();

        PrintWriter errWriter = commandLine.getErr();

        // Print the error message in red (if colors are enabled)
        errWriter.println(commandLine.getColorScheme().errorText(String.format("ERROR: %s", ex.getMessage())));

        if (isVerbose(fullParseResult)) {
            ex.printStackTrace(errWriter);
        } else {
            errWriter.println(commandLine.getColorScheme().stackTraceText("Run with [-v|--verbose] for full stack trace."));
        }
        return exitCode;
    }

    /**
     * Recursively checks if the verbose option was specified in the command or any of its subcommands.
     *
     * @param result The parse result to inspect.
     * @return {@code true} if {@code -v} or {@code --verbose} was matched; {@code false} otherwise.
     */
    private boolean isVerbose(CommandLine.ParseResult result) {
        return result.hasMatchedOption('v') || result.hasMatchedOption("verbose")
                || (result.hasSubcommand() && isVerbose(result.subcommand()));
    }
}
