package org.jgroups.raft.cli.commands;

import java.util.Scanner;

import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Abstract base class for sensitive commands that require user confirmation before execution.
 *
 * <p>
 * This class adds a safety layer to prevent accidental execution of critical operations (e.g., changing cluster membership,
 * removing nodes). By default, it prompts the user for interactive confirmation (Y/N) before proceeding.
 * </p>
 *
 * <p>
 * The prompt can be bypassed using the {@code --force} flag, which is useful for automated scripts.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
abstract class ConfirmationPromptCommand extends BaseProbeCommand {

    /**
     * Flag to bypass the interactive confirmation prompt.
     * Use this when running in non-interactive environments (scripts, CI/CD).
     */
    @Option(names = "--force", description = "Skip interactive confirmation prompt. (${DEFAULT-VALUE})", defaultValue = "false")
    private boolean force;

    /**
     * Defines the specific prompt message to display to the user.
     *
     * <p>
     * Example: "Are you sure you want to remove node A? [y/N]:"
     * </p>
     *
     * @return The prompt string.
     */
    protected abstract String promptMessage();

    /**
     * Executes the command with a safety check.
     *
     * <p>
     * The execution flow is:
     * <ol>
     *   <li>If {@code --force} is set, proceed immediately to {@link BaseProbeCommand#execute()}.</li>
     *   <li>Otherwise, display the {@link #promptMessage()} and wait for user input.</li>
     *   <li>If the user confirms (types 'y' or 'yes'), proceed to execute.</li>
     *   <li>If the user denies or provides invalid input, abort and print a message to {@code stderr}.</li>
     * </ol>
     */
    @Override
    protected void execute() {
        if (force) {
            super.execute();
            return;
        }

        if (hasUserConfirmed()) {
            super.execute();
            return;
        }

        err().println("User not confirmed execution of command. Exiting...");
    }

    /**
     * Handles the interactive confirmation logic.
     *
     * <p>
     * Prints the prompt to {@code stdout} and reads a line from {@code stdin}.
     * </p>
     *
     * @return {@code true} if the user input starts with "y" (case-insensitive); {@code false} otherwise.
     */
    private boolean hasUserConfirmed() {
        out().print(CommandLine.Help.Ansi.AUTO.string(String.format("%s [y/@|bold N|@]: ", promptMessage())));
        out().flush();

        // Utilizing Scanner like this allows user to pipe the answer in:
        // echo y | ./bin/raft ...
        Scanner scanner = new Scanner(System.in);
        if (scanner.hasNextLine()) {
            String input = scanner.nextLine().trim();
            return input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes");
        }

        return false;
    }
}
