package org.jgroups.raft.internal.statemachine;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.internal.serialization.Serializer;

final class StateMachineSnapshotter<T> {
    private final T concrete;
    private final Snapshotter snapshotter;
    private final Serializer serializer;

    StateMachineSnapshotter(T concrete, Serializer serializer) {
        this.concrete = concrete;
        this.serializer = serializer;

        Class<?> clazz = concrete.getClass();
        Map<Integer, Field> fields = new HashMap<>();
        for (Field field : clazz.getDeclaredFields()) {
            StateMachineField state = field.getAnnotation(StateMachineField.class);
            if (state == null) continue;

            Field prev = fields.put(state.order(), field);
            if (prev != null) {
                throw new IllegalStateException(String.format("State machine %s has multiple fields with order %d: %s and %s",
                        clazz.getName(), state.order(), prev.getName(), field.getName()));
            }

            int modifier = field.getModifiers();
            if (Modifier.isFinal(modifier))
                throw new IllegalStateException(String.format("State machine %s field %s is final, which is not allowed to load snapshots",
                        clazz.getName(), field.getName()));
        }

        this.snapshotter = new Snapshotter(fields);
    }

    byte[] writeSnapshot() {
        StateMachineStateHolder holder = snapshotter.write();
        return serializer.serialize(holder);
    }

    void readSnapshot(byte[] snapshot) {
        StateMachineStateHolder holder = serializer.deserialize(snapshot);
        snapshotter.read(holder);
    }

    private final class Snapshotter {
        private final Map<Integer, Field> fields;

        private Snapshotter(Map<Integer, Field> fields) {
            this.fields = fields;
        }

        private StateMachineStateHolder write() {
            Map<Integer, Object> state = new HashMap<>();
            for (Map.Entry<Integer, Field> entry : fields.entrySet()) {
                Field field = entry.getValue();
                field.setAccessible(true);
                try {
                    Object value = field.get(concrete);
                    state.put(entry.getKey(), value);
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(String.format("Failed to access field %s in state machine %s", field.getName(), concrete.getClass().getName()), e);
                }
            }
            return new StateMachineStateHolder(state);
        }

        private void read(StateMachineStateHolder holder) {
            for (Map.Entry<Integer, Field> entry : fields.entrySet()) {
                Field field = entry.getValue();
                field.setAccessible(true);
                Object value = holder.state().get(entry.getKey());
                try {
                    field.set(concrete, value);
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(String.format("Failed to set field %s in state machine %s", field.getName(), concrete.getClass().getName()), e);
                }
            }
        }
    }
}
