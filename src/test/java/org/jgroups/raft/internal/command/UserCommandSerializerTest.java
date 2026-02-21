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
 * Tests for {@link UserCommand} binary serialization.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class UserCommandSerializerTest extends AbstractBinarySerializerTest {

    @Override
    protected SingleBinarySerializer<?>[] serializers() {
        return new SingleBinarySerializer[] {
                UserCommand.SERIALIZER,
        };
    }

    @Test(dataProvider = "userCommands")
    public void testSerializationRoundTrip(UserCommand command) {
        assertSerializationRoundTrip(command, UserCommand.class);
    }

    @Test
    public void testSerializerMetadata() {
        SingleBinarySerializer<UserCommand> serializer = UserCommand.SERIALIZER;
        assertSerializerMetadata(
            serializer,
            UserCommand.class,
            RaftTypeIds.USER_COMMAND,
            (byte) 0
        );
    }

    @Test
    public void testWireFormat() {
        UserCommand command = new UserCommand(42L, 1, false);

        byte[] bytes = serialize(command);

        // Verify wire format: [type-id: int][version: byte][length: int][id: long][version: int][isRead: boolean]
        assertWireFormat(bytes, RaftTypeIds.USER_COMMAND, (byte) 0, 13);

        // Expected size: 4 (type-id) + 1 (version) + 4 (length) + 8 (id) + 4 (version) + 1 (boolean) = 18 bytes
        assertThat(bytes.length).isEqualTo(22);
    }

    @Test
    public void testReadCommand() {
        UserCommand readCommand = new UserCommand(1L, 1, true);

        assertThat(readCommand.isRead()).isTrue();
        assertThat(readCommand).isInstanceOf(JRaftReadCommand.class);

        // Verify serialization preserves the read flag
        assertSerializationRoundTrip(readCommand, UserCommand.class);
    }

    @Test
    public void testWriteCommand() {
        UserCommand writeCommand = new UserCommand(2L, 1, false);

        assertThat(writeCommand.isRead()).isFalse();
        assertThat(writeCommand).isInstanceOf(JRaftWriteCommand.class);

        // Verify serialization preserves the write flag
        assertSerializationRoundTrip(writeCommand, UserCommand.class);
    }

    @Test
    public void testMinMaxValues() {
        UserCommand maxValues = new UserCommand(Long.MAX_VALUE, Integer.MAX_VALUE, true);
        assertSerializationRoundTrip(maxValues, UserCommand.class);

        UserCommand minValues = new UserCommand(Long.MIN_VALUE, Integer.MIN_VALUE, false);
        assertSerializationRoundTrip(minValues, UserCommand.class);
    }

    @Test
    public void testZeroValues() {
        UserCommand zeros = new UserCommand(0L, 0, false);
        assertSerializationRoundTrip(zeros, UserCommand.class);
    }

    @DataProvider
    static Object[][] userCommands() {
        return new Object[][] {
            { new UserCommand(0L, 0, false) },
            { new UserCommand(1L, 1, true) },
            { new UserCommand(42L, 5, false) },
            { new UserCommand(Long.MAX_VALUE, Integer.MAX_VALUE, true) },
            { new UserCommand(Long.MIN_VALUE, Integer.MIN_VALUE, false) },
            { new UserCommand(12345L, 99, true) },
            { new UserCommand(999L, 1, false) },
        };
    }
}
