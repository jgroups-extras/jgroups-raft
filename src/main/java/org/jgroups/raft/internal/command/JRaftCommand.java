package org.jgroups.raft.internal.command;

import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * JRaftCommand is a factory to create commands to submit in {@link JGroupsRaft}.
 *
 * <p>
 * We will use this factory to generate command instances that identify the specific {@link StateMachine} method to invoke.
 * These markers are stateless and can be statically created during initialization. Commands will be categorized as either read or write,
 * which should be generated using the appropriate factory methods.
 * </p>
 *
 * <p>
 * We will ensure that commands remain backward compatible by assigning unique command IDs and versions to each command. Our approach
 * involves deprecating old commands before removing them, ensuring seamless transitions when reading commands from logs and applying them to
 * the correct method in the state machine. It is essential that command creation remains synchronized with methods in the state machine.
 * </p>
 *
 * <p>
 * Each command will be uniquely identified by its ID. The version argument will facilitate updates and additions of new commands to evolve a
 * state machine. Input and output types will be utilized for validation during both compilation and runtime.
 * </p>
 *
 * <b>Example:</b>
 * <pre>{@code
 * long GET_KEY_ID = 1;
 * JRaftCommand<String, String> GET_COMMAND = JRaftCommand.read(GET_KEY_ID, 1, String.class, String.class);
 * JRaft<?> raft = ...;
 * raft.submit(GET_COMMAND, "key", 10, TimeUnit.SECONDS);}
 * </pre>
 *
 * @since 2.0
 * @author José Bolina
 * @see JRaftReadCommand
 * @see JRaftWriteCommand
 */
public sealed interface JRaftCommand permits JRaftCommand.UserCommand, JRaftReadCommand, JRaftWriteCommand {

    /**
     * The command id.
     *
     * <p>
     * The id is unique for each command. The id is utilized to identify the correct method in the {@link StateMachine}
     * to invoke.
     * </p>
     *
     * @return The command id.
     */
    long id();

    /**
     * The command version.
     *
     * <p>
     * The version is utilized to evolve the state machine with new commands. The version, together with the id, will
     * identify the correct method in the state machine to invoke.
     * </p>
     *
     * @return The command version.
     */
    int version();

    @ProtoTypeId(ProtoStreamTypes.USER_COMMAND)
    final class UserCommand implements JRaftCommand, JRaftReadCommand, JRaftWriteCommand {

        private final long id;
        private final int version;
        private final boolean read;

        @ProtoFactory
        UserCommand(long id, int version, boolean read) {
            this.id = id;
            this.version = version;
            this.read = read;
        }

        @ProtoField(number = 1, defaultValue = "0")
        public long id() {
            return id;
        }

        @ProtoField(number = 2, defaultValue = "0")
        public int version() {
            return version;
        }

        @ProtoField(number = 3, defaultValue = "false")
        public boolean isRead() {
            return read;
        }

        @Override
        public boolean equals(Object object) {
            if (object == null || getClass() != object.getClass()) return false;
            UserCommand that = (UserCommand) object;
            return id == that.id
                    && version == that.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, version);
        }

        @Override
        public String toString() {
            return "UserCommand{" +
                    "id=" + id +
                    ", version=" + version +
                    '}';
        }
    }
}
