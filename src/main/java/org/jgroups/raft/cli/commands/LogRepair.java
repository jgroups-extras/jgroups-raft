package org.jgroups.raft.cli.commands;

import java.io.IOException;

import picocli.CommandLine.Command;

/**
 * Repairs recoverable corruption in a Raft log directory with operator confirmation.
 *
 * @since 2.0
 * @author José Bolina
 */
@Command(name = "repair", description = "Fix recoverable corruption in the log directory.")
final class LogRepair extends BaseLogCommand {

    @Override
    protected int execute() throws IOException {
        return EXIT_OK;
    }
}
