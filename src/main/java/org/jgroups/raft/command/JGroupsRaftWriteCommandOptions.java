package org.jgroups.raft.command;

import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

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

    @ProtoTypeId(ProtoStreamTypes.WRITE_COMMAND_OPTIONS)
    final class WriteImpl implements JGroupsRaftWriteCommandOptions {
        private final boolean ignoreReturnValue;

        @ProtoFactory
        WriteImpl(boolean ignoreReturnValue) {
            this.ignoreReturnValue = ignoreReturnValue;
        }

        @ProtoField(number = 1, defaultValue = "false")
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
    }
}
