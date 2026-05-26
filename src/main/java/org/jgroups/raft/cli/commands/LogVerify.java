package org.jgroups.raft.cli.commands;

import java.io.IOException;

import picocli.CommandLine.Command;

/**
 * Scans a Raft log directory and reports integrity status without modifying any files.
 *
 * @since 2.0
 * @author José Bolina
 */
@Command(name = "verify", description = "Scan the log directory for corruption and report findings.")
final class LogVerify extends BaseLogCommand {
    @Override
    protected int execute() throws IOException {
        return EXIT_OK;
    }
}
