package org.jgroups.raft.internal.command;

import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.internal.command.JRaftCommand.UserCommand;
import org.jgroups.raft.internal.serialization.ObjectWrapper;
import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypes.RAFT_COMMAND)
public final class RaftCommand {

    private final UserCommand command;
    private final Object[] input;
    private final JGroupsRaftCommandOptions options;

    public RaftCommand(JRaftCommand command, Object[] input, JGroupsRaftCommandOptions options) {
        this.command = (UserCommand) command;
        this.input = input;
        this.options = options;
    }

    @ProtoFactory
    RaftCommand(UserCommand userCommand, List<ObjectWrapper<Object>> inputWrapper, ObjectWrapper<JGroupsRaftCommandOptions> optionsWrapper) {
        this(userCommand, inputWrapper.stream().map(ObjectWrapper::unwrap).toArray(), ObjectWrapper.unwrap(optionsWrapper));
    }

    public JRaftCommand command() {
        return command;
    }

    @ProtoField(number = 1, name = "command")
    UserCommand userCommand() {
        return command;
    }

    public Object[] input() {
        return input;
    }

    @ProtoField(number = 2, name = "input", collectionImplementation = ArrayList.class)
    List<ObjectWrapper<Object>> inputWrapper() {
        if (input == null || input.length == 0) {
            return null;
        }

        return Stream.of(input)
                .map(ObjectWrapper::create)
                .toList();
    }

    @ProtoField(number = 3, name = "options")
    ObjectWrapper<JGroupsRaftCommandOptions> optionsWrapper() {
        return ObjectWrapper.create(options);
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
}
