package org.jgroups.raft.internal;

import java.util.function.Function;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Follower;
import org.jgroups.protocols.raft.Leader;
import org.jgroups.protocols.raft.Learner;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftAdministration;
import org.jgroups.raft.JGroupsRaftMetrics;
import org.jgroups.raft.JGroupsRaftRole;
import org.jgroups.raft.JGroupsRaftState;
import org.jgroups.raft.Settable;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.command.JGroupsRaftWriteCommandOptions;
import org.jgroups.raft.exceptions.JRaftException;
import org.jgroups.raft.internal.metrics.GroupsRaftMetricsCollector;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.internal.statemachine.StateMachineWrapper;

/**
 * Default implementation of the {@link JGroupsRaft} interface.
 *
 * @param <T> the state machine type
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaft
 */
final class JGroupsRaftImpl<T> implements JGroupsRaft<T> {

    private final JGroupsRaftParameters<T> parameters;
    private final boolean attachedChannel;
    private final Serializer serializer;
    private final CommandRegistry<T> registry;
    private final StateMachineWrapper<T> wrapper;

    private Settable settable;
    private JGroupsRaftMetrics metrics;
    private JGroupsRaftState state;
    private JGroupsRaftAdministration administration;
    private volatile JGroupsRaftRole role;
    private volatile boolean started;

    JGroupsRaftImpl(JGroupsRaftParameters<T> parameters) {
        this.parameters = parameters;
        this.attachedChannel = !parameters.channel().isConnected();
        this.registry = new CommandRegistry<>(parameters.sm(), parameters.api());
        this.serializer = Serializer.protoStream(parameters.registry());
        this.wrapper = new StateMachineWrapper<>(parameters.sm(), parameters.api(), registry, serializer);
        this.role = JGroupsRaftRole.NONE;
    }

    @Override
    public void start() {
        if (started) return;

        JChannel channel = parameters.channel();

        // A JChannel can not be re-utilized after it is closed.
        // The channel only gets in this state after explicitly calling the close method.
        if (channel.isClosed()) {
            throw new IllegalStateException("Instance can not be utilized after it was closed");
        }

        // Find the RAFT protocol for general management and metric gathering.
        RAFT raft;
        if ((raft = RAFT.findProtocol(RAFT.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("RAFT protocol was not found");

        // Search for the first protocol in the stack implementing Settable.
        // For example, we might have REDIRECT in the stack to send the command to the correct RAFT leader.
        if ((settable = RAFT.findProtocol(Settable.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("did not find a protocol implementing Settable (e.g. REDIRECT or RAFT)");

        // Query the current role and then register a listener.
        // In case the channel is already initialized, we wouldn't receive the initial role update.
        handleRoleUpdate(raft.role());
        raft.addRoleListener(this::handleRoleUpdate);

        // Initialize the command registry based on the given state machine.
        // The initialization will parse the methods and generate the appropriate schemas for the annotated methods.
        // We'll utilize the new schema and compare with a previous version, if present, in the node's log.
        registry.initialize();
        wrapper.initialize(settable);
        raft.stateMachine(wrapper);

        // Only instantiate metric collection if it was enabled during startup.
        boolean metricsEnabled = parameters.runtimeProperties().getBoolean(JGroupsRaftMetrics.METRICS_ENABLED);
        metrics = metricsEnabled
                ? new GroupsRaftMetricsCollector(raft, null)
                : JGroupsRaftMetrics.disabled();

        // The state is just a view over the RAFT protocol.
        // None of the values should be writable from the outside.
        state = new JGroupsRaftState() {
            @Override
            public String id() {
                return raft.raftId();
            }

            @Override
            public String leader() {
                return raft.leaderRaftId();
            }

            @Override
            public long term() {
                return raft.currentTerm();
            }

            @Override
            public long commitIndex() {
                return raft.commitIndex();
            }

            @Override
            public long lastApplied() {
                return raft.lastAppended();
            }
        };
        administration = JGroupsRaftAdministrationImpl.create(channel);

        // Only connect in case the channel is still disconnected.
        // A provided JChannel might be already connected.
        if (!parameters.channel().isConnected()) {
            try {
                parameters.channel().connect(parameters.clusterName());
            } catch (Exception e) {
                throw new JRaftException("Failed to connect to cluster", e);
            }
        }

        started = true;
    }

    private void handleRoleUpdate(String clazz) {
        if (clazz.equals(Leader.class.getSimpleName())) {
            handleRoleUpdate(Role.Leader);
            return;
        }

        if (clazz.equals(Follower.class.getSimpleName())) {
            handleRoleUpdate(Role.Follower);
            return;
        }

        if (clazz.equals(Learner.class.getSimpleName())) {
            handleRoleUpdate(Role.Learner);
            return;
        }

        role = JGroupsRaftRole.NONE;
    }

    private void handleRoleUpdate(Role r) {
        this.role = switch (r) {
            case Leader -> JGroupsRaftRole.LEADER;
            case Follower -> JGroupsRaftRole.FOLLOWER;
            case Learner -> JGroupsRaftRole.LEARNER;
        };
    }

    @Override
    public void stop() {
        if (!started) return;

        started = false;
        registry.destroy();

        // If the channel is created by the RAFT instance, we close it.
        // If the channel was provided, it is the application's responsibility to close it.
        if (attachedChannel) {
            JChannel channel = parameters.channel();
            channel.close();
        }

        // Reset the role to identify it was not started.
        role = JGroupsRaftRole.NONE;
    }

    @Override
    public JGroupsRaftRole role() {
        return role;
    }

    @Override
    public <O> O write(Function<T, O> function, JGroupsRaftWriteCommandOptions options) {
        ensureInstanceInitialized();
        return wrapper.submit(function, options);
    }

    @Override
    public <O> O read(Function<T, O> function, JGroupsRaftReadCommandOptions options) {
        ensureInstanceInitialized();
        return wrapper.submit(function, options);
    }

    @Override
    public T readOnly(JGroupsRaftReadCommandOptions options) {
        ensureInstanceInitialized();
        return wrapper.createWrapper(options, StateMachineRead.class);
    }

    @Override
    public T writeOnly(JGroupsRaftWriteCommandOptions options) {
        ensureInstanceInitialized();
        return wrapper.createWrapper(options, StateMachineWrite.class);
    }

    @Override
    public JGroupsRaftAdministration administration() {
        ensureInstanceInitialized();
        return administration;
    }

    @Override
    public JGroupsRaftState state() {
        ensureInstanceInitialized();
        return state;
    }

    private void ensureInstanceInitialized() {
        if (!started)
            throw new IllegalStateException("JGroupsRaft instance is not started");
    }
}
