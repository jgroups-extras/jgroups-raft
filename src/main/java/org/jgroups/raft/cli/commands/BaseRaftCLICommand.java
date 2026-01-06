package org.jgroups.raft.cli.commands;

import java.io.PrintWriter;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * The abstract base class for all JGroups Raft CLI commands.
 *
 * <p>
 * This class provides the foundational infrastructure required by every command in the hierarchy. It encapsulates
 * standard behavior such as:
 *
 * <ul>
 *   <li>Standard options like {@code --help} and {@code --verbose}.</li>
 *   <li>Access to the Picocli {@link CommandSpec} for metadata and configuration.</li>
 *   <li>Convenience methods for writing to the standard output ({@code stdout}) and error ({@code stderr}) streams.</li>
 * </ul>
 * </p>
 *
 * All concrete commands should extend this class (or one of its subclasses) rather than implementing
 * {@link Runnable} directly.
 *
 * @since 2.0
 * @author José Bolina
 */
abstract class BaseRaftCLICommand implements Runnable {

    /**
     * The command specification injected by Picocli.
     * Use this to access the command line model, ANSI settings, and output streams.
     */
    @Spec
    private CommandSpec spec;

    /**
     * Standard option to display the usage help message.
     * <p>
     * If specified, Picocli handles printing the help automatically (via {@code usageHelp = true}), bypassing the
     * command's execution logic.
     * </p>
     */
    @Option(names = {"-h", "--help"}, description = "Display this help message.", usageHelp = true)
    protected boolean help;

    /**
     * Global option to enable verbose logging.
     *
     * <p>
     * This flag is detected by the exception handler to print full stack traces upon errors, and can be checked by
     * subclasses via {@link #isVerbose()} to print debug information.
     * </p>
     */
    @Option(names = {"-v", "--verbose"}, description = "Enable verbose output")
    private boolean verbose;

    /**
     * Returns the Picocli command specification.
     *
     * @return The {@link CommandSpec} associated with this command instance.
     */
    protected final CommandSpec spec() {
        return spec;
    }

    /**
     * Checks whether the verbose mode is enabled.
     *
     * @return {@code true} if the user provided the {@code -v} or {@code --verbose} flag.
     */
    public final boolean isVerbose() {
        return verbose;
    }

    /**
     * Returns the standard output writer configured for this command.
     *
     * <p>
     * Commands should always use this writer instead of {@code System.out} to ensure proper testing and integration
     * with the CLI framework.
     * </p>
     *
     * @return The {@link PrintWriter} for {@code stdout}.
     */
    protected final PrintWriter out() {
        return spec.commandLine().getOut();
    }

    /**
     * Returns the standard error writer configured for this command.
     *
     * <p>
     * Commands should always use this writer instead of {@code System.err} for reporting non-fatal warnings or errors.
     * </p>
     *
     * @return The {@link PrintWriter} for {@code stderr}.
     */
    protected final PrintWriter err() {
        return spec.commandLine().getErr();
    }
}
