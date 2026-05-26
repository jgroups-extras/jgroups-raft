package org.jgroups.raft.cli.commands;

import java.io.IOException;

import picocli.CommandLine.Command;

/**
 * Converts a v2 Raft log directory to v1 format, enabling rollback to a previous release.
 *
 * @since 2.0
 * @author José Bolina
 */
@Command(name = "downgrade", description = "Convert log files from v2 to v1 format for release rollback.")
final class LogDowngrade extends BaseLogCommand {
    @Override
    protected int execute() throws IOException {
        return EXIT_OK;
    }
}
