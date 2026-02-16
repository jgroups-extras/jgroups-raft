package org.jgroups.raft.command;

/**
 * Customizes how commands behave during execution.
 *
 * <p>
 * This interface provides options to modify command execution behavior on a per-invocation basis. Different options are
 * available for read and write operations to optimize performance and consistency based on application requirements.
 * </p>
 *
 * <p>
 * Common options include:
 * <ul>
 * <li><b>Ignore return value:</b> Avoids serializing and returning response data, improving performance when results are not needed</li>
 * <li><b>Linearizable reads:</b> Controls whether read operations require consensus or can be executed locally with potential stale data</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaftReadCommandOptions
 * @see JGroupsRaftWriteCommandOptions
 */
public sealed interface JGroupsRaftCommandOptions permits JGroupsRaftReadCommandOptions, JGroupsRaftWriteCommandOptions {

    /**
     * Whether to ignore the return value of the command execution.
     *
     * <p>
     * When {@code true}, the response will not be serialized and returned to the caller, which can improve performance
     * for fire-and-forget operations.
     * </p>
     *
     * @return {@code true} if return value should be ignored, {@code false} otherwise
     */
    boolean ignoreReturnValue();

    /**
     * Creates a builder for read command options.
     *
     * @return a new read options builder
     */
    static ReadBuilder readOptions() {
        return new ReadBuilder();
    }

    /**
     * Creates a builder for write command options.
     *
     * @return a new write options builder
     */
    static WriteBuilder writeOptions() {
        return new WriteBuilder();
    }

    /**
     * Builder for read command options.
     */
    final class ReadBuilder {
        private boolean ignoreReturnValue;
        private boolean linearizable = true;

        /**
         * Sets whether to ignore the return value of the read operation.
         *
         * @param ignore {@code true} to ignore return values, {@code false} otherwise
         * @return this builder
         */
        public ReadBuilder ignoreReturnValue(boolean ignore) {
            this.ignoreReturnValue = ignore;
            return this;
        }

        /**
         * Sets whether the read operation should be linearizable.
         *
         * <p>
         * When {@code true}, the read goes through the consensus protocol ensuring linearizable consistency. When
         * {@code false}, the read is executed locally and may return stale data but with better performance.
         * </p>
         *
         * @param linearizable {@code true} for linearizable reads, {@code false} for local reads
         * @return this builder
         */
        public ReadBuilder linearizable(boolean linearizable) {
            this.linearizable = linearizable;
            return this;
        }

        /**
         * Builds the read command options.
         *
         * @return the configured read options
         */
        public JGroupsRaftReadCommandOptions build() {
            return new JGroupsRaftReadCommandOptions.ReadImpl(linearizable, ignoreReturnValue);
        }
    }

    /**
     * Builder for write command options.
     */
    final class WriteBuilder {
        private boolean ignoreReturnValue;

        /**
         * Sets whether to ignore the return value of the write operation.
         *
         * @param ignore {@code true} to ignore return values, {@code false} otherwise
         * @return this builder
         */
        public WriteBuilder ignoreReturnValue(boolean ignore) {
            this.ignoreReturnValue = ignore;
            return this;
        }

        /**
         * Builds the write command options.
         *
         * @return the configured write options
         */
        public JGroupsRaftWriteCommandOptions build() {
            return new JGroupsRaftWriteCommandOptions.WriteImpl(ignoreReturnValue);
        }
    }
}
