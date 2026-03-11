package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.JRaftCommand.UserCommand;
import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;

import java.util.Arrays;
import java.util.Objects;

final class RaftCommand {
    public static final SingleBinarySerializer<RaftCommand> SERIALIZER =  RaftCommandSerializer.INSTANCE;

    private final UserCommand command;
    private final Object[] input;
    private final JGroupsRaftCommandOptions options;

    public RaftCommand(JRaftCommand command, Object[] input, JGroupsRaftCommandOptions options) {
        this.command = (UserCommand) command;
        this.input = input;
        this.options = options;
    }

    public JRaftCommand command() {
        return command;
    }

    public Object[] input() {
        return input;
    }

    public boolean isRead() {
        return command.isRead();
    }

    public JGroupsRaftCommandOptions options() {
        return options;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass()) return false;
        RaftCommand that = (RaftCommand) object;
        return Objects.equals(command, that.command)
                && Arrays.equals(input, that.input)
                && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, Arrays.hashCode(input));
    }

    @Override
    public String toString() {
        return "RaftCommand{" +
                "command=" + command +
                ", input=" + Arrays.toString(input) +
                ", options=" + options +
                '}';
    }

    private static final class RaftCommandSerializer implements SingleBinarySerializer<RaftCommand> {
        private static final RaftCommandSerializer INSTANCE = new RaftCommandSerializer();

        private RaftCommandSerializer() { }

        @Override
        public void write(SerializationContextWrite ctx, RaftCommand target) {
            ctx.writeObject(target.command);

            if (target.input == null) {
                ctx.writeInt(-1);
            } else {
                ctx.writeInt(target.input.length);
                for (Object o : target.input) {
                    ctx.writeObject(o);
                }
            }

            ctx.writeObject(target.options);
        }

        @Override
        public RaftCommand read(SerializationContextRead ctx, byte ignore) {
            UserCommand command = ctx.readObject();
            int length = ctx.readInt();
            Object[] input = null;
            if (length >= 0) {
                input = new Object[length];
                for (int i = 0; i < length; i++) {
                    input[i] = ctx.readObject();
                }
            }
            JGroupsRaftCommandOptions options = ctx.readObject();
            return new  RaftCommand(command, input, options);
        }

        @Override
        public Class<RaftCommand> javaClass() {
            return RaftCommand.class;
        }

        @Override
        public int type() {
            return RaftTypeIds.RAFT_COMMAND;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
