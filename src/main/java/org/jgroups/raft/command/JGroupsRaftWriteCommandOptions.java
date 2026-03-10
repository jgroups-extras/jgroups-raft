package org.jgroups.raft.command;

import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.serialization.JGroupsRaftCustomMarshaller;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.Objects;

/**
 * Command options for write operations.
 *
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaftCommandOptions
 */
public sealed interface JGroupsRaftWriteCommandOptions extends JGroupsRaftCommandOptions permits JGroupsRaftWriteCommandOptions.WriteImpl {

    /**
     * Creates a builder for write command options.
     *
     * @return a new write options builder
     */
    static WriteBuilder options() {
        return JGroupsRaftCommandOptions.writeOptions();
    }

    final class WriteImpl implements JGroupsRaftWriteCommandOptions {
        public static final JGroupsRaftCustomMarshaller<?> SERIALIZER = ReadImplSerializer.INSTANCE;
        private final boolean ignoreReturnValue;

        WriteImpl(boolean ignoreReturnValue) {
            this.ignoreReturnValue = ignoreReturnValue;
        }

        @Override
        public boolean ignoreReturnValue() {
            return ignoreReturnValue;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            WriteImpl write = (WriteImpl) o;
            return ignoreReturnValue == write.ignoreReturnValue;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(ignoreReturnValue);
        }

        private static final class ReadImplSerializer implements JGroupsRaftCustomMarshaller<WriteImpl> {

            private static final ReadImplSerializer INSTANCE = new ReadImplSerializer();

            private ReadImplSerializer() { }

            @Override
            public void write(SerializationContextWrite ctx, WriteImpl target) {
                ctx.writeBoolean(target.ignoreReturnValue());
            }

            @Override
            public WriteImpl read(SerializationContextRead ctx, byte version) {
                boolean ignoreReturnValue = ctx.readBoolean();
                return (WriteImpl) options()
                        .ignoreReturnValue(ignoreReturnValue)
                        .build();
            }

            @Override
            public Class<WriteImpl> javaClass() {
                return WriteImpl.class;
            }

            @Override
            public int type() {
                return RaftTypeIds.WRITE_COMMAND_OPTIONS;
            }

            @Override
            public byte version() {
                return 0;
            }
        }
    }
}
