package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.internal.serialization.Serializer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Automates the extraction and restoration of a state machine's internal state for automatic Raft snapshots.
 *
 * <p>
 * This utility relies on Java Reflection to scan the concrete state machine instance for fields annotated with
 * {@link StateMachineField}. It maps these fields based on their defined order and provides mechanisms to easily serialize
 * the current state into a byte array, or deserialize a byte array to overwrite the current fields.
 *
 * @since 2.0
 * @author José Bolina
 * @see StateMachineField
 * @param <T> The type of the concrete state machine instance.
 */
final class StateMachineSnapshotter<T> {
    private final T concrete;
    private final Snapshotter snapshotter;
    private final Serializer serializer;

    /**
     * Constructs the snapshotter and pre-computes the reflection metadata.
     *
     * <p>
     * During instantiation, this validates the state machine's structure, ensuring that:
     * <ul>
     *   <li>No two annotated fields share the same order ID.</li>
     *   <li>No annotated field is declared as {@code final}, as they must be mutable during restoration.</li>
     * </ul>
     * </p>
     *
     * @param concrete   The concrete state machine instance.
     * @param serializer The serializer used to encode/decode the {@link StateMachineStateHolder}.
     * @throws IllegalStateException If duplicate order IDs or final fields are detected.
     */
    StateMachineSnapshotter(T concrete, Serializer serializer) {
        this.concrete = concrete;
        this.serializer = serializer;

        Class<?> clazz = concrete.getClass();
        Map<Integer, Field> fields = new HashMap<>();

        // Traverse the full hierarchy up to the Object.class to get all annotated fields.
        while (clazz != null && clazz != Object.class) {
            for (Field field : clazz.getDeclaredFields()) {
                StateMachineField state = field.getAnnotation(StateMachineField.class);
                if (state == null) continue;

                Field prev = fields.put(state.order(), field);
                if (prev != null) {
                    throw new IllegalStateException(String.format("State machine %s has multiple fields with order %d: %s and %s",
                            clazz.getName(), state.order(), prev.getName(), field.getName()));
                }

                int modifier = field.getModifiers();
                String fullFieldName = String.format("%s#%s", clazz.getName(), field.getName());
                if (Modifier.isFinal(modifier)) {
                    throw new IllegalStateException(String.format("State machine %s has field %s with final modifier. The modifier is not allowed to load snapshots",
                            concrete.getClass().getName(), fullFieldName));
                }

                if (Modifier.isStatic(modifier)) {
                    throw new IllegalStateException(String.format("State machine %s has field %s with static modifier. The modifier is not allowed to load snapshots",
                            concrete.getClass().getName(), fullFieldName));
                }
            }
            clazz = clazz.getSuperclass();
        }

        this.snapshotter = new Snapshotter(fields);
    }

    /**
     * Serializes the state machine snapshot.
     *
     * <p>
     * Extracts the annotated fields from the concrete state machine and packages them into a {@link StateMachineStateHolder},
     * and serializes the result into a byte array.
     * </p>
     *
     * @return A byte array representing the serialized snapshot of the state machine.
     */
    byte[] writeSnapshot() {
        StateMachineStateHolder holder = snapshotter.write();
        return serializer.serialize(holder);
    }

    /**
     * Restores the state machine from a snapshot.
     *
     * <p>
     * Deserializes a snapshot byte array into a {@link StateMachineStateHolder} and uses reflection to overwrite the
     * annotated fields of the concrete state machine with the loaded values.
     * </p>
     *
     * @param snapshot The byte array containing the serialized state.
     */
    void readSnapshot(byte[] snapshot) {
        StateMachineStateHolder holder = serializer.deserialize(snapshot);
        snapshotter.read(holder);
    }

    /**
     * Internal helper class responsible for the direct reflection operations on the state machine instance.
     */
    private final class Snapshotter {
        private final Map<Integer, Field> fields;

        private Snapshotter(Map<Integer, Field> fields) {
            this.fields = fields;
        }

        /**
         * Reads the current values of all mapped fields via reflection.
         *
         * @return A holder containing the mapped field orders and their current values.
         */
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

        /**
         * Writes the provided values into the mapped fields via reflection.
         *
         * @param holder The holder containing the snapshot data to restore.
         */
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
