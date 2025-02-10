package org.jgroups.raft.internal.command;

import org.jgroups.raft.JGroupsRaft;

/**
 * // TODO: Document this interface
 *
 * // Explain why write commands are treated differently.
 *
 * @since 2.0
 * @author José Bolina
 */
public sealed interface JRaftWriteCommand extends JRaftCommand permits JRaftCommand.UserCommand {

    /**
     * Creates a new command to be used in the {@link JGroupsRaft} instance.
     *
     * <p>
     * This factory method creates a write command to apply to the state machine.
     * </p>
     *
     * @param id The command id.
     * @param version The command version.
     * @return A new instance of a command to submit to the state machine.
     */
    static JRaftWriteCommand create(long id, int version) {
        return new JRaftCommand.UserCommand(id, version, false);
    }
}
