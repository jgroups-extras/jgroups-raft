package org.jgroups.raft.command;

import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

/**
 * Command options for read operations.
 *
 * <p>
 * Extends the base command options with read-specific behavior such as linearizable consistency control. Read operations
 * can choose between strong consistency (linearizable) or better performance (local reads).
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaftCommandOptions
 */
public sealed interface JGroupsRaftReadCommandOptions extends JGroupsRaftCommandOptions permits JGroupsRaftReadCommandOptions.ReadImpl {

    /**
     * Creates a builder for read command options.
     *
     * @return a new read options builder
     */
    static ReadBuilder options() {
        return JGroupsRaftCommandOptions.readOptions();
    }

    /**
     * Whether this read operation should be linearizable.
     *
     * <p>
     * Linearizable reads go through the consensus protocol to ensure strong consistency but have higher latency.
     * Non-linearizable reads are executed locally and may return stale data but offer better performance.
     * </p>
     *
     * @return {@code true} for linearizable reads, {@code false} for local reads
     */
    boolean linearizable();

    @ProtoTypeId(ProtoStreamTypes.READ_COMMAND_OPTIONS)
    final class ReadImpl implements JGroupsRaftReadCommandOptions {
        private final boolean linearizable;
        private final boolean ignoreReturnValue;

        @ProtoFactory
        ReadImpl(boolean linearizable, boolean ignoreReturnValue) {
            this.linearizable = linearizable;
            this.ignoreReturnValue = ignoreReturnValue;
        }

        @ProtoField(number = 1, defaultValue = "true")
        @Override
        public boolean linearizable() {
            return linearizable;
        }

        @ProtoField(number = 2, defaultValue = "false")
        @Override
        public boolean ignoreReturnValue() {
            return ignoreReturnValue;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ReadImpl read = (ReadImpl) o;
            return linearizable == read.linearizable
                    && ignoreReturnValue == read.ignoreReturnValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(linearizable, ignoreReturnValue);
        }
    }
}
