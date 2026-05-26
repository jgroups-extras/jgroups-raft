package org.jgroups.raft.cli.commands;

import picocli.CommandLine.Command;

/**
 * Command group for offline Raft log file operations.
 *
 * <p>
 * These commands operate directly on files in a stopped node's log directory. They do not connect to a running cluster.
 * Running any subcommand against a directory owned by a running node is prevented by a lock file mechanism.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@Command(
        name = "log",
        description = "Inspect, repair, or downgrade offline Raft log files.",
        subcommands = {
                LogVerify.class,
                LogRepair.class,
                LogDowngrade.class,
        }
)
final class LogCommand extends BaseRaftCLICommand {
    @Override
    public void run() {
        spec().commandLine().usage(out());
    }
}
