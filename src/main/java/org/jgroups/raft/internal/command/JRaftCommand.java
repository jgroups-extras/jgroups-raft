package org.jgroups.raft.internal.command;

import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.Objects;

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
    int id();

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

    final class UserCommand implements JRaftCommand, JRaftReadCommand, JRaftWriteCommand {
        public static final SingleBinarySerializer<UserCommand> SERIALIZER = UserCommandSerializer.INSTANCE;

        private final int id;
        private final int version;
        private final boolean read;

        UserCommand(int id, int version, boolean read) {
            this.id = id;
            this.version = version;
            this.read = read;
        }

        @Override
        public int id() {
            return id;
        }

        @Override
        public int version() {
            return version;
        }

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

        private static final class UserCommandSerializer implements SingleBinarySerializer<UserCommand> {
            private static final UserCommandSerializer INSTANCE = new UserCommandSerializer();

            private UserCommandSerializer() { }

            @Override
            public void write(SerializationContextWrite ctx, UserCommand target) {
                ctx.writeInt(target.id);
                ctx.writeInt(target.version);
                ctx.writeBoolean(target.isRead());
            }

            @Override
            public UserCommand read(SerializationContextRead ctx, byte ignore) {
                int id = ctx.readInt();
                int version = ctx.readInt();
                boolean read = ctx.readBoolean();
                return new UserCommand(id, version, read);
            }

            @Override
            public Class<UserCommand> javaClass() {
                return UserCommand.class;
            }

            @Override
            public int type() {
                return RaftTypeIds.USER_COMMAND;
            }

            @Override
            public byte version() {
                return 0;
            }
        }
    }
}
