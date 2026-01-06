package org.jgroups.raft.cli.commands;

import picocli.CommandLine;

/**
 * Registry for all available JGroups Raft CLI commands.
 *
 * <p>
 * This class serves as the single source of truth for the commands exposed by the CLI tool. By centralizing the command
 * registration here, we decouple the main entry point ({@link org.jgroups.raft.cli.JGroupsRaftCLI}) from the concrete
 * command implementations.
 * </p>
 *
 * <p>
 * New commands should be added to the {@link #CLI_COMMANDS} array to be automatically picked up by the application bootstrapper.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public final class JGroupsRaftCLICommands {

    /**
     * An array of all command classes to be registered as subcommands.
     *
     * <p>
     * Includes:
     * <ul>
     * <li>Standard Picocli help/completion utilities.</li>
     * <li>Core Raft operations (Status, Metrics, Membership).</li>
     * </ul>
     * </p>
     */
    public static final Class<?>[] CLI_COMMANDS = new Class[] {
            CommandLine.HelpCommand.class,
            Completion.class,
            Status.class,
            Member.class,
            Metrics.class,
            RawProbeCommand.class,
    };
}
