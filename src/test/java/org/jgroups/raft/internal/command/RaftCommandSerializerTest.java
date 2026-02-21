package org.jgroups.raft.internal.command;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.command.JRaftCommand.UserCommand;
import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.internal.serialization.binary.AbstractBinarySerializerTest;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for {@link RaftCommand} binary serialization.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RaftCommandSerializerTest extends AbstractBinarySerializerTest {

    @Override
    protected SingleBinarySerializer<?>[] serializers() {
        // RaftCommand depends on UserCommand, so register both
        return new SingleBinarySerializer[] {
                UserCommand.SERIALIZER,
                RaftCommand.SERIALIZER,
        };
    }

    @Test(dataProvider = "raftCommands")
    public void testSerializationRoundTrip(RaftCommand command) {
        assertSerializationRoundTrip(command, RaftCommand.class);
    }

    @Test
    public void testSerializerMetadata() {
        SingleBinarySerializer<RaftCommand> serializer = RaftCommand.SERIALIZER;

        assertSerializerMetadata(
            serializer,
            RaftCommand.class,
            RaftTypeIds.RAFT_COMMAND,
            (byte) 0
        );
    }

    @Test
    public void testWireFormat() {
        UserCommand userCommand = new UserCommand(1L, 1, false);
        RaftCommand command = new RaftCommand(userCommand, null, null);

        byte[] bytes = serialize(command);

        // Verify wire format starts correctly
        assertWireFormat(bytes, RaftTypeIds.RAFT_COMMAND, (byte) 0, 30);
    }

    @Test
    public void testNullInput() {
        UserCommand userCommand = new UserCommand(1L, 1, false);
        RaftCommand command = new RaftCommand(userCommand, null, null);

        assertSerializationRoundTrip(command, RaftCommand.class);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);
        assertThat(deserialized.input()).isNull();
    }

    @Test
    public void testEmptyInput() {
        UserCommand userCommand = new UserCommand(1L, 1, false);
        RaftCommand command = new RaftCommand(userCommand, new Object[0], null);

        assertSerializationRoundTrip(command, RaftCommand.class);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);
        assertThat(deserialized.input()).isNotNull();
        assertThat(deserialized.input()).isEmpty();
    }

    @Test
    public void testNullOptions() {
        UserCommand userCommand = new UserCommand(1L, 1, false);
        RaftCommand command = new RaftCommand(userCommand, null, null);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);
        assertThat(deserialized.options()).isNull();
    }

    @Test
    public void testCommandEquality() {
        UserCommand userCommand = new UserCommand(42L, 5, true);
        RaftCommand command1 = new RaftCommand(userCommand, null, null);
        RaftCommand command2 = new RaftCommand(userCommand, null, null);

        // Commands with same data should be equal
        assertThat(command1).isEqualTo(command2);

        // Serialized and deserialized should be equal
        RaftCommand deserialized = deserialize(serialize(command1), RaftCommand.class);
        assertThat(deserialized).isEqualTo(command1);
    }

    @Test
    public void testReadCommand() {
        UserCommand readCommand = new UserCommand(1L, 1, true);
        RaftCommand command = new RaftCommand(readCommand, null, null);

        assertThat(command.isRead()).isTrue();

        assertSerializationRoundTrip(command, RaftCommand.class);
    }

    @Test
    public void testWriteCommand() {
        UserCommand writeCommand = new UserCommand(2L, 1, false);
        RaftCommand command = new RaftCommand(writeCommand, null, null);

        assertThat(command.isRead()).isFalse();

        assertSerializationRoundTrip(command, RaftCommand.class);
    }

    @DataProvider
    static Object[][] raftCommands() {
        UserCommand cmd1 = new UserCommand(1L, 1, false);
        UserCommand cmd2 = new UserCommand(42L, 5, true);
        UserCommand cmd3 = new UserCommand(Long.MAX_VALUE, Integer.MAX_VALUE, false);

        return new Object[][] {
            // Null inputs and options
            { new RaftCommand(cmd1, null, null) },

            // Empty input array
            { new RaftCommand(cmd2, new Object[0], null) },

            // Different command types
            { new RaftCommand(cmd3, null, null) },

            // Various command configurations
            { new RaftCommand(new UserCommand(0L, 0, false), null, null) },
            { new RaftCommand(new UserCommand(999L, 99, true), new Object[0], null) },
        };
    }

    /**
     * Test cases for RaftCommand with primitive input types.
     * Note: This requires primitive type serializers to be registered.
     * If not yet implemented, these tests will be skipped or fail gracefully.
     */
    @Test(enabled = false, description = "Requires primitive type serializers")
    public void testWithPrimitiveInputs() {
        UserCommand userCommand = new UserCommand(1L, 1, false);

        // String input
        RaftCommand withString = new RaftCommand(userCommand, new Object[]{"test"}, null);
        assertSerializationRoundTrip(withString, RaftCommand.class);

        // Integer input
        RaftCommand withInt = new RaftCommand(userCommand, new Object[]{42}, null);
        assertSerializationRoundTrip(withInt, RaftCommand.class);

        // Multiple inputs
        RaftCommand withMultiple = new RaftCommand(userCommand, new Object[]{"key", 100L}, null);
        assertSerializationRoundTrip(withMultiple, RaftCommand.class);
    }

    /**
     * Test case for RaftCommand with byte array input.
     * Note: This requires byte[] serializer to be registered.
     */
    @Test(enabled = false, description = "Requires byte[] serializer")
    public void testWithByteArrayInput() {
        UserCommand userCommand = new UserCommand(1L, 1, false);
        byte[] data = new byte[]{1, 2, 3, 4, 5};

        RaftCommand command = new RaftCommand(userCommand, new Object[]{data}, null);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);

        assertThat(deserialized.input()).isNotNull();
        assertThat(deserialized.input()).hasSize(1);
        assertThat(deserialized.input()[0]).isInstanceOf(byte[].class);
        assertThat((byte[]) deserialized.input()[0]).containsExactly(1, 2, 3, 4, 5);
    }
}
