package org.jgroups.raft.cli.commands;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Manages the Raft protocol membership configuration.
 *
 * <p>
 * This command suite allows for dynamic reconfiguration of the Raft cluster, which involves adding or removing voting
 * members from the consensus group.
 *
 * <p>
 * <b>Important Distinction:</b>
 * These operations affect the <i>Raft voting membership</i>, which is distinct from the underlying JGroups cluster view.
 * A node can be part of the JGroups cluster (physically connected) without being a voting member of the Raft algorithm
 * (e.g., a {@link org.jgroups.protocols.raft.Learner node}).
 * </p>
 *
 * Membership changes require a functioning leader and a quorum of existing nodes to commit the configuration change log entry.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
@Command(
        name = "member",
        description = "Execute membership changes in the Raft cluster. Observe the changes refer to the Raft membership and not cluster membership.",
        subcommands = {
                Member.Add.class,
                Member.Remove.class,
                Member.List.class,
        }
)
final class Member extends BaseRaftCLICommand {

    /**
     * Default execution logic for the root 'member' command.
     *
     * <p>
     * Displays usage information since this command requires a subcommand.
     * </p>
     */
    @Override
    public void run() {
        spec().commandLine().usage(spec().commandLine().getOut());
    }

    /**
     * Subcommand to dynamically add a new server to the Raft configuration.
     *
     * <p>
     * This operation triggers a configuration change in the Raft log. If successful, the new node will become a voting
     * member ({@link org.jgroups.protocols.raft.Follower}) and will start receiving log entries.
     * </p>
     *
     * <p>
     * <b>Warning:</b> This command requires interactive confirmation unless {@code --force} is used.
     * </p>
     */
    @Command(name = "add", description = "Dynamically add a new member to the Raft membership list.")
    static final class Add extends ConfirmationPromptCommand {

        @Parameters(index = "0", description = "The Raft ID of the node to add")
        private String raftId;

        @Override
        protected String probeRequest() {
            return String.format("RAFT.addServer[%s]", raftId);
        }

        @Override
        protected String promptMessage() {
            return String.format("Do you confirm ADDING node '%s'", raftId);
        }
    }

    /**
     * Subcommand to dynamically remove a server from the Raft configuration.
     *
     * <p>
     * Removing a node reduces the quorum size required for future commits.
     * <b>Warning:</b> Removing too many nodes or removing the current leader can disrupt availability.
     * </p>
     *
     * <p>
     * <b>Warning:</b> This command requires interactive confirmation unless {@code --force} is used.
     * </p>
     */
    @Command(name = "remove", description = "Dynamically removes a member of the Raft membership list.")
    static final class Remove extends ConfirmationPromptCommand {

        @Parameters(index = "0", description = "The Raft ID of the node to remove")
        private String raftId;

        @Override
        protected String probeRequest() {
            return String.format("RAFT.removeServer[%s]", raftId);
        }

        @Override
        protected String promptMessage() {
            return String.format("Do you confirm REMOVING node '%s'", raftId);
        }
    }

    /**
     * Subcommand to retrieve the current list of voting Raft members.
     *
     * <p>
     * This queries the leader's current configuration to show which nodes are actively participating in the consensus
     * algorithm. The output does <b>not</b> include {@link org.jgroups.protocols.raft.Learner} nodes.
     * </p>
     */
    @Command(name = "list", description = "List all nodes in the Raft membership list.")
    static final class List extends BaseProbeCommand {

        @Override
        protected String probeRequest() {
            return "RAFT.members";
        }
    }
}
