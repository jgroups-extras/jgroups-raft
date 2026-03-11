package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

import java.util.HashMap;
import java.util.Map;

/**
 * Hold the internal state of a Raft state machine during snapshot operations for serialization and deserialization.
 *
 * <p>
 * This class encapsulates a mapping between a field's unique {@link StateMachineField#order()} and its
 * actual runtime value.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
record StateMachineStateHolder(Map<Integer, Object> state) {
    public static final SingleBinarySerializer<StateMachineStateHolder> SERIALIZER = StateMachineStateHolderSerializer.INSTANCE;

    private static final class StateMachineStateHolderSerializer implements SingleBinarySerializer<StateMachineStateHolder> {
        private static final StateMachineStateHolderSerializer INSTANCE = new StateMachineStateHolderSerializer();

        private StateMachineStateHolderSerializer() { }

        @Override
        public void write(SerializationContextWrite ctx, StateMachineStateHolder target) {
            int size = target.state.size();
            ctx.writeInt(size);
            if (size == 0)
                return;

            for (Map.Entry<Integer, Object> entry : target.state.entrySet()) {
                ctx.writeInt(entry.getKey());
                ctx.writeObject(entry.getValue());
            }
        }

        @Override
        public StateMachineStateHolder read(SerializationContextRead ctx, byte version) {
            int size = ctx.readInt();
            if (size == 0) {
                return new StateMachineStateHolder(Map.of());
            }

            HashMap<Integer, Object> state = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                state.put(ctx.readInt(), ctx.readObject());
            }
            return new StateMachineStateHolder(state);
        }

        @Override
        public Class<StateMachineStateHolder> javaClass() {
            return StateMachineStateHolder.class;
        }

        @Override
        public int type() {
            return RaftTypeIds.STATE_MACHINE_STATE_HOLDER;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
