package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.StateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.RaftCommand;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.registry.ReplicatedMethodWrapper;
import org.jgroups.raft.internal.serialization.Serializer;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * The internal state machine that applies successfully committed Raft operations to the concrete state machine implementation.
 *
 * <p>
 * This class implements the core {@link StateMachine} and is invoked exclusively by the Raft consensus protocol.
 * It is responsible for deserializing incoming logs, mapping them to the correct local method, applying the execution,
 * and managing cluster snapshots.
 * </p>
 *
 * <p>
 * Operations applied through the {@link StateMachine#apply(byte[], int, int, boolean)} method are guaranteed to be
 * invoked by a single thread.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @param <T> The type of the concrete state machine implementation.
 */
final class StateMachineAdapter<T> implements StateMachine {
    private final T delegate;
    private final CommandRegistry<T> registry;
    private final Serializer serializer;
    private final StateMachineSnapshotter<T> snapshotter;

    StateMachineAdapter(Serializer serializer, CommandRegistry<T> registry, T delegate) {
        this.registry = registry;
        this.serializer = serializer;
        this.delegate = delegate;
        this.snapshotter = new StateMachineSnapshotter<>(delegate, serializer);
    }

    /**
     * Callback invoked by JGroups Raft when a log entry has been committed by the cluster.
     *
     * <p>
     * This method maps the serialized payload back into a {@link ReplicatedMethodWrapper} and strictly coordinates
     * thread-safety via synchronization to ensure concurrent, non-linearizable client reads do not witness partial state mutation.
     * </p>
     *
     * @param data               The serialized byte array from the Raft log.
     * @param offset             The offset within the byte array.
     * @param length             The length of the serialized data.
     * @param serialize_response True if the result of the method should be serialized and returned.
     * @return The serialized result of the local method execution, or null.
     */
    @Override
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
        RaftCommand rc = serializer.deserialize(data);
        JRaftCommand command = rc.command();

        if (command == null)
            throw new IllegalStateException("command cannot be null");

        ReplicatedMethodWrapper method = registry.getCommand(command.id());
        Object res;

        // CRITICAL: Synchronize over the concrete implementation of the state machine.
        // Raft guarantees that committed commands passed to apply() are executed sequentially.
        // However, non-linearizable read operations (handled in StateMachineInvocationHandler) bypass the Raft log and
        // execute concurrently on the state machine.
        // This lock ensures thread-safety between ordered write mutations and fast dirty reads.
        synchronized (delegate) {
            res = method.submit(rc.input());
        }

        if (res == null)
            return null;

        JGroupsRaftCommandOptions options = rc.options();
        return options == null || !options.ignoreReturnValue()
                ? serializer.serialize(res)
                : null;
    }

    @Override
    public void readContentFrom(DataInput in) throws Exception {
        int length = in.readInt();
        byte[] snapshot = new byte[length];
        in.readFully(snapshot);
        snapshotter.readSnapshot(snapshot);
    }

    @Override
    public void writeContentTo(DataOutput out) throws Exception {
        byte[] snapshot = snapshotter.writeSnapshot();
        out.writeInt(snapshot.length);
        out.write(snapshot);
    }
}
