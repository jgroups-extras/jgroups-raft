package org.jgroups.raft.configuration;

import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.util.pattern.NestedBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Nested builder to configure the underlying {@link org.jgroups.protocols.raft.RAFT} protocol.
 *
 * <p>
 * This builder is obtained through {@link JGroupsRaft.Builder#configureRaft()} and is used to override
 * RAFT protocol properties programmatically. Settings defined here take precedence over the values declared
 * in the JGroups XML configuration file.
 * </p>
 *
 * <p>
 * Not all properties need to be set. Only the properties explicitly configured will override the defaults.
 * Properties that are not set remain as declared in the configuration file.
 * </p>
 *
 * <h2>Usage</h2>
 *
 * <p>
 * The builder follows a nested builder pattern. After configuring the RAFT protocol properties, call
 * {@link #and()} to return to the parent {@link JGroupsRaft.Builder} and continue with the remaining
 * configuration.
 * </p>
 *
 * <pre>{@code
 * JGroupsRaft<MyStateMachine> raft = JGroupsRaft.builder(stateMachine, MyStateMachine.class)
 *     .withJGroupsConfig("raft.xml")
 *     .withClusterName("my-cluster")
 *     .configureRaft()
 *         .withRaftId("node-1")
 *         .withMembers(List.of("node-1", "node-2", "node-3"))
 *         .withLogDirectory("/var/raft/log")
 *         .and()
 *     .build();
 * }</pre>
 *
 * @param <T> the type of the state machine, inherited from the parent builder.
 * @since 2.0
 * @see JGroupsRaft.Builder#configureRaft()
 * @see org.jgroups.protocols.raft.RAFT
 */
public final class RaftProtocolBuilder<T> implements NestedBuilder<Void, JGroupsRaft<T>, JGroupsRaft.Builder<T>> {

    private final JGroupsRaft.Builder<T> parent;
    private String raftId;
    private Class<?> logClass;
    private String logArgs;
    private String logDirectory;
    private String logPrefix;
    private Long resendInterval;
    private Boolean sendCommitsImmediately;
    private Long maxLogSize;
    private Boolean useFsync;
    private Collection<String> members;

    public RaftProtocolBuilder(JGroupsRaft.Builder<T> parent) {
        this.parent = parent;
    }

    /**
     * Defines the Raft ID for this node.
     *
     * <p>
     * The Raft ID uniquely identifies this node within the cluster. Every node in the cluster must have
     * a distinct Raft ID, and the value must match one of the entries in the members list. If not set,
     * the value from the configuration file is used.
     * </p>
     *
     * @param raftId the unique identifier for this node in the Raft cluster.
     * @return this builder instance.
     */
    public RaftProtocolBuilder<T> withRaftId(String raftId) {
        this.raftId = raftId;
        return this;
    }

    /**
     * Defines the implementation class to use for the Raft log.
     *
     * <p>
     * The log class must implement the {@code Log} interface. If not set, the default log implementation
     * from the configuration file is used.
     * </p>
     *
     * @param logClass the class that implements the Raft log.
     * @return this builder instance.
     * @see #withLogArgs(String)
     */
    public RaftProtocolBuilder<T> withLogClass(Class<?> logClass) {
        this.logClass = logClass;
        return this;
    }

    /**
     * Defines initialization arguments passed to the log implementation.
     *
     * <p>
     * The arguments are passed to the log implementation at initialization time. The expected format
     * depends on the specific log class in use. If not set, the value from the configuration file is used.
     * </p>
     *
     * @param logArgs initialization arguments for the log implementation.
     * @return this builder instance.
     * @see #withLogClass(Class)
     */
    public RaftProtocolBuilder<T> withLogArgs(String logArgs) {
        this.logArgs = logArgs;
        return this;
    }

    /**
     * Defines the directory where the Raft log files are stored.
     *
     * <p>
     * The directory must exist and be writable. If not set, the value from the configuration file is used.
     * </p>
     *
     * @param logDirectory the path to the log directory.
     * @return this builder instance.
     */
    public RaftProtocolBuilder<T> withLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
        return this;
    }

    /**
     * Defines the prefix for Raft log file names.
     *
     * <p>
     * The prefix is prepended to each log file name. This is useful when multiple Raft instances share
     * the same log directory. If not set, the value from the configuration file is used.
     * </p>
     *
     * @param logPrefix the prefix to use for log file names.
     * @return this builder instance.
     */
    public RaftProtocolBuilder<T> withLogPrefix(String logPrefix) {
        this.logPrefix = logPrefix;
        return this;
    }

    /**
     * Defines the interval, in milliseconds, at which uncommitted log entries are re-sent to followers.
     *
     * <p>
     * This value controls how frequently the leader retransmits log entries that have not yet been
     * acknowledged by followers. Lowering this value can reduce recovery latency at the cost of increased
     * network traffic. If not set, the value from the configuration file is used.
     * </p>
     *
     * @param resendInterval the retransmission interval in milliseconds.
     * @return this builder instance.
     */
    public RaftProtocolBuilder<T> withResendInterval(long resendInterval) {
        this.resendInterval = resendInterval;
        return this;
    }

    /**
     * Controls whether commit notifications are sent to followers immediately upon commit.
     *
     * <p>
     * When {@code true}, the leader sends a commit notification to followers as soon as an entry is
     * committed, rather than waiting for the next append-entries message. Enabling this reduces commit
     * latency on followers at the cost of additional network messages. If not set, the value from the
     * configuration file is used.
     * </p>
     *
     * @param sendCommitsImmediately {@code true} to send commit notifications immediately.
     * @return this builder instance.
     */
    public RaftProtocolBuilder<T> withSendCommitsImmediately(boolean sendCommitsImmediately) {
        this.sendCommitsImmediately = sendCommitsImmediately;
        return this;
    }

    /**
     * Defines the maximum size, in bytes, of the Raft log before a snapshot is triggered.
     *
     * <p>
     * When the log exceeds this threshold, the Raft protocol will attempt to compact the log by taking
     * a snapshot of the current state machine. If not set, the value from the configuration file is used.
     * </p>
     *
     * @param maxLogSize the maximum log size in bytes.
     * @return this builder instance.
     */
    public RaftProtocolBuilder<T> withMaxLogSize(long maxLogSize) {
        this.maxLogSize = maxLogSize;
        return this;
    }

    /**
     * Controls whether {@code fsync} is called after each log write.
     *
     * <p>
     * When {@code true}, each write to the log is followed by an {@code fsync} call, which guarantees
     * that data is flushed to durable storage before the write is acknowledged. This improves durability
     * at the cost of write throughput. If not set, the value from the configuration file is used.
     * </p>
     *
     * @param useFsync {@code true} to enable {@code fsync} on every log write.
     * @return this builder instance.
     */
    public RaftProtocolBuilder<T> withUseFsync(boolean useFsync) {
        this.useFsync = useFsync;
        return this;
    }

    /**
     * Defines the fixed set of node IDs that form the Raft cluster.
     *
     * <p>
     * The members list determines the voting quorum. Each entry must match the Raft ID of a node in the
     * cluster as set via {@link #withRaftId(String)}. The list is copied defensively and cannot be
     * modified after this call. If not set, the value from the configuration file is used.
     * </p>
     *
     * @param members the collection of Raft node IDs that constitute the cluster.
     * @return this builder instance.
     * @throws NullPointerException if {@code members} is null.
     */
    public RaftProtocolBuilder<T> withMembers(Collection<String> members) {
        Objects.requireNonNull(members, "members cannot be null");
        this.members = List.copyOf(members);
        return this;
    }

    /**
     * Applies all configured properties to the given {@link RAFT} protocol instance.
     *
     * <p>
     * This method is invoked internally by {@link JGroupsRaft.Builder#build()} and is not intended to be
     * called directly. Only properties that were explicitly set will be applied; unset properties are
     * left unchanged on the {@code RAFT} instance.
     * </p>
     *
     * @param raft the {@link RAFT} protocol instance to configure.
     */
    public void build(RAFT raft) {
        if (raftId != null) raft.raftId(raftId);
        if (logClass != null) raft.logClass(logClass.getCanonicalName());
        if (logArgs != null) raft.logArgs(logArgs);
        if (logDirectory != null) raft.logDir(logDirectory);
        if (logPrefix != null) raft.logPrefix(logPrefix);
        if (resendInterval != null) raft.resendInterval(resendInterval);
        if (sendCommitsImmediately != null) raft.sendCommitsImmediately(sendCommitsImmediately);
        if (maxLogSize != null) raft.maxLogSize(maxLogSize);
        if (useFsync != null) raft.logUseFsync(useFsync);
        if (members != null && !members.isEmpty()) raft.members(members);
    }

    /**
     * Returns to the parent {@link JGroupsRaft.Builder} to continue configuration.
     *
     * @return the parent builder instance.
     */
    @Override
    public JGroupsRaft.Builder<T> and() {
        return parent;
    }

    /**
     * Not supported. Use {@link #build(RAFT)} to apply settings, followed by {@link #and()} to return
     * to the parent builder.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override
    public Void build() {
        throw new UnsupportedOperationException("This builder does not produce a RAFT result. Use the build(RAFT raft) method instead.");
    }
}
