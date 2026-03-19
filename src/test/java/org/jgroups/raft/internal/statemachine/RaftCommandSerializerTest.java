package org.jgroups.raft.internal.statemachine;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.internal.command.JRaftCommand.UserCommand;
import org.jgroups.raft.internal.command.JRaftReadCommand;
import org.jgroups.raft.internal.command.JRaftWriteCommand;
import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.internal.serialization.binary.AbstractBinarySerializerTest;
import org.jgroups.raft.internal.serialization.binary.SerializationRegistry;

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
    void testSerializationRoundTrip(RaftCommand command) {
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
        JRaftReadCommand userCommand = JRaftReadCommand.create(1, 1);
        RaftCommand command = new RaftCommand(userCommand, null, null);

        byte[] bytes = serialize(command);

        // Verify wire format starts correctly
        assertWireFormat(bytes, RaftTypeIds.RAFT_COMMAND, (byte) 0, 26);
    }

    @Test
    public void testNullInput() {
        JRaftReadCommand userCommand = JRaftReadCommand.create(1, 1);
        RaftCommand command = new RaftCommand(userCommand, null, null);

        assertSerializationRoundTrip(command, RaftCommand.class);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);
        assertThat(deserialized.input()).isNull();
    }

    @Test
    public void testEmptyInput() {
        JRaftReadCommand userCommand = JRaftReadCommand.create(1, 1);
        RaftCommand command = new RaftCommand(userCommand, new Object[0], null);

        assertSerializationRoundTrip(command, RaftCommand.class);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);
        assertThat(deserialized.input()).isNotNull();
        assertThat(deserialized.input()).isEmpty();
    }

    @Test
    public void testNullOptions() {
        JRaftReadCommand userCommand = JRaftReadCommand.create(1, 1);
        RaftCommand command = new RaftCommand(userCommand, null, null);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);
        assertThat(deserialized.options()).isNull();
    }

    @Test
    public void testCommandEquality() {
        JRaftWriteCommand userCommand = JRaftWriteCommand.create(42, 5);
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
        JRaftReadCommand readCommand = JRaftReadCommand.create(1, 1);
        RaftCommand command = new RaftCommand(readCommand, null, null);

        assertThat(command.isRead()).isTrue();

        assertSerializationRoundTrip(command, RaftCommand.class);
    }

    @Test
    public void testWriteCommand() {
        JRaftWriteCommand writeCommand = JRaftWriteCommand.create(2, 1);
        RaftCommand command = new RaftCommand(writeCommand, null, null);

        assertThat(command.isRead()).isFalse();

        assertSerializationRoundTrip(command, RaftCommand.class);
    }

    /**
     * Compatibility test for RaftCommand wire format.
     *
     * <p>
     * Ensures that the binary serialization format remains stable across versions.
     * If this test fails, the wire format has changed — see {@code SerializationCompatibilityTest}
     * for the update procedure.
     * </p>
     */
    @Test
    public void testSerializationCompatibility() {
        Serializer serializer = Serializer.create(SerializationRegistry.create());

        RaftCommand command = new RaftCommand(
                JRaftReadCommand.create(1, 1),
                new Object[]{"arg1", 42},
                JGroupsRaftReadCommandOptions.options().linearizable(true).build()
        );

        byte[] expectedBytes = hexToBytes("00 00 00 41 00 00 00 00 33 00 00 00 43 00 00 00 00 09 00 00 00 01 00 00 00 01 01 00 00 00 02 00 00 00 08 00 04 61 72 67 31 00 00 00 02 00 00 00 2A 00 00 00 44 00 00 00 00 02 01 00");

        byte[] actualBytes = serializer.serialize(command);
        assertThat(actualBytes)
                .withFailMessage("RaftCommand wire format has changed")
                .isEqualTo(expectedBytes);

        Object deserialized = serializer.deserialize(expectedBytes, RaftCommand.class);
        assertThat(deserialized).isEqualTo(command);
    }

    @DataProvider
    static Object[][] raftCommands() {
        JRaftWriteCommand cmd1 = JRaftWriteCommand.create(1, 1);
        JRaftReadCommand cmd2 = JRaftReadCommand.create(42, 5);
        JRaftWriteCommand cmd3 = JRaftWriteCommand.create(Integer.MAX_VALUE, Integer.MAX_VALUE);

        return new Object[][] {
            // Null inputs and options
            { new RaftCommand(cmd1, null, null) },

            // Empty input array
            { new RaftCommand(cmd2, new Object[0], null) },

            // Different command types
            { new RaftCommand(cmd3, null, null) },

            // Various command configurations
            { new RaftCommand(JRaftWriteCommand.create(0, 0), null, null) },
            { new RaftCommand(JRaftReadCommand.create(999, 99), new Object[0], null) },
        };
    }

    /**
     * Test cases for RaftCommand with primitive input types.
     * Note: This requires primitive type serializers to be registered.
     * If not yet implemented, these tests will be skipped or fail gracefully.
     */
    @Test(enabled = false, description = "Requires primitive type serializers")
    public void testWithPrimitiveInputs() {
        JRaftWriteCommand userCommand = JRaftWriteCommand.create(1, 1);

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
        JRaftWriteCommand userCommand = JRaftWriteCommand.create(1, 1);
        byte[] data = new byte[]{1, 2, 3, 4, 5};

        RaftCommand command = new RaftCommand(userCommand, new Object[]{data}, null);

        RaftCommand deserialized = deserialize(serialize(command), RaftCommand.class);

        assertThat(deserialized.input()).isNotNull();
        assertThat(deserialized.input()).hasSize(1);
        assertThat(deserialized.input()[0]).isInstanceOf(byte[].class);
        assertThat((byte[]) deserialized.input()[0]).containsExactly(1, 2, 3, 4, 5);
    }

    private static byte[] hexToBytes(String hex) {
        hex = hex.replaceAll("\\s+", "");
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}
