package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.StateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.exceptions.JGroupsRaftException;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.RaftResponse;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.registry.ReplicatedMethodWrapper;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.util.ByteArrayDataInputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;

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
     * @throws RuntimeException If the state machine method fails. The failure is never returned as a response, since
     *                          a deterministic state machine must not fail while applying a committed command.
     */
    @Override
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
        RaftCommand rc = serializer.deserialize(data, RaftCommand.class);
        JRaftCommand command = rc.command();

        if (command == null)
            throw new IllegalStateException("command cannot be null");

        ReplicatedMethodWrapper method = registry.getCommand(command.id(), command.version());

        Object r;
        try {
            r = submit(rc, method);
        }  catch (InvocationTargetException e) {
            // The state machine must be deterministic, a failure is never a valid result.
            // Propagating it lets RAFT enter degraded state instead of applying more entries and cause bigger problems.
            throw propagate(e.getCause());
        } catch (IllegalAccessException e) {
            throw new JGroupsRaftException("Failed to invoke state machine method: " + method, e);
        }

        if (r == null)
            return null;

        JGroupsRaftCommandOptions options = rc.options();
        return options == null || !options.ignoreReturnValue()
                ? serializer.serialize(new RaftResponse(r))
                : null;
    }

    private Object submit(RaftCommand rc, ReplicatedMethodWrapper method) throws InvocationTargetException, IllegalAccessException {
        // CRITICAL: Synchronize over the concrete implementation of the state machine.
        // Raft guarantees that committed commands passed to apply() are executed sequentially.
        // However, non-linearizable read operations (handled in StateMachineInvocationHandler) bypass the Raft log and
        // execute concurrently on the state machine.
        // This lock ensures thread-safety between ordered write mutations and fast dirty reads.
        synchronized (delegate) {
            return method.submit(rc.input());
        }
    }

    /**
     * Restores the state machine from a snapshot.
     *
     * <p>
     * A failure to read the snapshot is propagated instead of leaving the state machine untouched.
     * </p>
     *
     * @param in The snapshot content.
     * @throws UncheckedIOException If reading the snapshot fails.
     */
    @Override
    public void readContentFrom(DataInput in) {
        byte[] buf;
        try {
            if (in instanceof ByteArrayDataInputStream badis) {
                int length = badis.available();
                buf = new byte[length];
                in.readFully(buf);
            } else {
                InputStream is = (InputStream) in;
                buf = is.readAllBytes();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read state machine snapshot", e);
        }
        snapshotter.readSnapshot(buf);
    }

    @Override
    public void writeContentTo(DataOutput out) {
        snapshotter.writeSnapshot(out);
    }

    private static RuntimeException propagate(Throwable t) {
        // Unchecked failures keep their type so the client sees what the state machine threw.
        if (t instanceof RuntimeException re)
            return re;

        if (t instanceof Error error)
            throw error;

        // Checked exception declared by the state machine method.
        return new JGroupsRaftException("State machine method failed", t);
    }
}
