package org.jgroups.raft.internal.command;

import org.jgroups.raft.JGroupsRaft;

/**
 * // TODO: Document this interface
 * // Explain how read commands are treated.
 *
 */
public sealed interface JRaftReadCommand extends JRaftCommand permits JRaftCommand.UserCommand {

    /**
     * Creates a new command to be used in the {@link JGroupsRaft} instance.
     *
     * <p>
     * This factory method creates a read command to apply to the state machine.
     * </p>
     *
     * @param id The command id.
     * @param version The command version.
     * @return A new instance of a command to submit to the state machine.
     */
    static JRaftReadCommand create(long id, int version) {
        return new JRaftCommand.UserCommand(id, version, true);
    }
}
