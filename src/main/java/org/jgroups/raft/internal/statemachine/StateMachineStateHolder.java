package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.internal.serialization.ObjectWrapper;
import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Hold the internal state of a Raft state machine during snapshot operations for serialization and deserialization.
 *
 * <p>
 * This class encapsulates a mapping between a field's unique {@link org.jgroups.raft.StateMachineField#order()} and its
 * actual runtime value. By leveraging {@link ObjectWrapper}, it safely handles the serialization of polymorphic objects
 * before delegating to ProtoStream.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@ProtoTypeId(ProtoStreamTypes.STATE_MACHINE_STATE_HOLDER)
public final class StateMachineStateHolder {

    private final Map<Integer, Object> state;

    @ProtoFactory
    static StateMachineStateHolder factory(Map<Integer, ObjectWrapper<Object>> internalState) {
        // Utilize a HashMap to accept null values.
        Map<Integer, Object> state = new HashMap<>();
        for (Map.Entry<Integer, ObjectWrapper<Object>> entry : internalState.entrySet()) {
            state.put(entry.getKey(), ObjectWrapper.unwrap(entry.getValue()));
        }
        return new StateMachineStateHolder(state);
    }

    public StateMachineStateHolder(Map<Integer, Object> state) {
        this.state = state;
    }

    /**
     * Exposes the state map to ProtoStream for serialization, automatically wrapping the raw objects into
     * {@link ObjectWrapper} instances to ensure complex or polymorphic types are properly encoded.
     *
     * @return A map of order IDs to wrapped objects.
     */
    @SuppressWarnings("rawtypes")
    @ProtoField(number = 1, name = "state", mapImplementation = HashMap.class)
    Map<Integer, ObjectWrapper> internalState() {
        return state.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> ObjectWrapper.create(e.getValue())));
    }

    public Map<Integer, Object> state() {
        return state;
    }
}
