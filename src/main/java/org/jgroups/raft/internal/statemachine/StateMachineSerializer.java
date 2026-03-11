package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;

import java.util.Collection;
import java.util.List;

public final class StateMachineSerializer {

    private StateMachineSerializer() { }

    private static final SingleBinarySerializer<?>[] SERIALIZERS = {
            StateMachineStateHolder.SERIALIZER,
            RaftCommand.SERIALIZER,
    };

    public static Collection<SingleBinarySerializer<?>> serializers() {
        return List.of(SERIALIZERS);
    }
}
