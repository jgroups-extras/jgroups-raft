package org.jgroups.raft.cli;

import org.jgroups.raft.cli.commands.JGroupsRaftCLICommands;
import org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler;

import java.util.function.Consumer;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * The main entry point for the JGroups Raft Command Line Interface (CLI).
 *
 * <p>
 * This class serves as the root command and bootstraps the CLI environment. It is responsible for:
 * <ul>
 * <li>Registering all available subcommands defined in {@link JGroupsRaftCLICommands}.</li>
 * <li>Configuring the global exception handler {@link JGroupsRaftExceptionHandler}.</li>
 * <li>Executing the user's input and managing the process exit code.</li>
 * </ul>
 * </p>
 *
 * If no arguments are provided, the default behavior is to print the usage information.
 *
 * @since 2.0
 * @author José Bolina
 */
@Command(
        name = "raft",
        mixinStandardHelpOptions = true,
        description = "JGroups Raft management CLI",
        versionProvider = JGroupsRaftCLI.ManifestVersionProvider.class
)
public class JGroupsRaftCLI implements Runnable {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    /**
     * The main method that bootstraps the CLI application.
     * <p>
     * It initializes the command hierarchy, sets up exception handling to ensure user-friendly
     * error messages (instead of raw stack traces), and executes the command logic.
     *
     * @param args The command line arguments passed by the user.
     */
    public static void main(String[] args) {
        int exitCode = internal(args, ignore -> {});
        System.exit(exitCode);
    }

    static int internal(String[] args, Consumer<CommandLine> decorator) {
        CommandLine cli = new CommandLine(new JGroupsRaftCLI());

        // Register all subcommands dynamically from the command registry.
        // Commands must be included in the registry to be available for users.
        for (Class<?> command : JGroupsRaftCLICommands.CLI_COMMANDS) {
            cli.addSubcommand(command);
        }

        // Install the custom exception handler for cleaner error reporting
        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());
        decorator.accept(cli);

        return cli.execute(args);
    }

    /**
     * Default execution logic when the root 'raft' command is run without subcommands.
     *
     * <p>
     * Displays the usage help message to guide the user on available commands.
     * </p>
     */
    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    static final class ManifestVersionProvider implements CommandLine.IVersionProvider {

        @Override
        public String[] getVersion() {
            String version = getClass().getPackage().getImplementationVersion();
            if (version == null) {
                version = "Development Build";
            }

            return new String[] {
                    "${COMMAND-FULL-NAME} version: " + version,
                    "JVM: " + System.getProperty("java.version"),
                    "Vendor: " + getClass().getPackage().getImplementationVendor()
            };
        }
    }
}
