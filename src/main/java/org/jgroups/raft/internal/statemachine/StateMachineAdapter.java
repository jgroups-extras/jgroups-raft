package org.jgroups.raft.internal.statemachine;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.raft.StateMachine;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.RaftCommand;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.registry.ReplicatedMethodWrapper;
import org.jgroups.raft.internal.serialization.Serializer;

final class StateMachineAdapter<T> implements StateMachine {
    private final CommandRegistry<T> registry;
    private final Serializer serializer;
    private final StateMachineSnapshotter<T> snapshotter;
    private final T delegate;

    StateMachineAdapter(Serializer serializer, CommandRegistry<T> registry, T delegate) {
        this.registry = registry;
        this.serializer = serializer;
        this.delegate = delegate;
        this.snapshotter = new StateMachineSnapshotter<>(delegate, serializer);
    }

    @Override
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
        RaftCommand rc = serializer.deserialize(data);
        JRaftCommand command = rc.command();

        if (command == null)
            throw new IllegalStateException("command cannot be null");

        ReplicatedMethodWrapper<?> method = registry.getCommand(command.id());
        Object res = method.submit(rc.input());
        if (res == null)
            return null;

        return serializer.serialize(res);
    }

    @Override
    public void readContentFrom(DataInput in) throws Exception {
        System.out.println("Adapter reading snapshot from");
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
