package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.internal.serialization.ObjectWrapper;
import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypes.STATE_MACHINE_STATE_HOLDER)
public final class StateMachineStateHolder {

    private final Map<Integer, Object> state;

    @ProtoFactory
    static StateMachineStateHolder factory(Map<Integer, ObjectWrapper<Object>> internalState) {
        Map<Integer, Object> map = internalState.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> ObjectWrapper.unwrap(e.getValue())));
        return new StateMachineStateHolder(map);
    }

    public StateMachineStateHolder(Map<Integer, Object> state) {
        this.state = state;
    }

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
