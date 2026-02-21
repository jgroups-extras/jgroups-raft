package org.jgroups.raft.command;

import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.serialization.JGroupsRaftCustomMarshaller;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.Objects;

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

    final class ReadImpl implements JGroupsRaftReadCommandOptions {
        public static final JGroupsRaftCustomMarshaller<?> SERIALIZER = ReadImplSerializer.INSTANCE;

        private final boolean linearizable;
        private final boolean ignoreReturnValue;

        ReadImpl(boolean linearizable, boolean ignoreReturnValue) {
            this.linearizable = linearizable;
            this.ignoreReturnValue = ignoreReturnValue;
        }

        @Override
        public boolean linearizable() {
            return linearizable;
        }

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

        private static final class ReadImplSerializer implements JGroupsRaftCustomMarshaller<ReadImpl> {
            private static final ReadImplSerializer INSTANCE = new ReadImplSerializer();

            private ReadImplSerializer() { }

            @Override
            public void write(SerializationContextWrite ctx, ReadImpl target) {
                ctx.writeBoolean(target.linearizable());
                ctx.writeBoolean(target.ignoreReturnValue());
            }

            @Override
            public ReadImpl read(SerializationContextRead ctx, byte version) {
                boolean linearizable = ctx.readBoolean();
                boolean ignoreReturnValue = ctx.readBoolean();
                return (ReadImpl) JGroupsRaftReadCommandOptions.options()
                        .linearizable(linearizable)
                        .ignoreReturnValue(ignoreReturnValue)
                        .build();
            }

            @Override
            public Class<ReadImpl> javaClass() {
                return ReadImpl.class;
            }

            @Override
            public int type() {
                return RaftTypeIds.READ_COMMAND_OPTIONS;
            }

            @Override
            public byte version() {
                return 0;
            }
        }
    }
}
