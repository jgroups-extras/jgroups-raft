package org.jgroups.raft;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field for inclusion in Raft snapshots.
 *
 * <p>
 * This annotation identifies state machine fields that should be persisted when creating snapshots of the current state.
 * Snapshots are used to compact the log and enable efficient state transfer to new or lagging nodes.
 * </p>
 *
 * <p>
 * <b>Field requirements:</b>
 * <ul>
 * <li>Must not be {@code private} - accessible for snapshot operations</li>
 * <li>Must not be {@code final} - modifiable during snapshot restoration</li>
 * <li>Must be thread-safe - concurrent access during snapshot creation</li>
 * </ul>
 * </p>
 *
 * <p>
 * The {@code order} parameter determines the sequence in which fields are stored and restored in snapshots, ensuring
 * consistent state reconstruction.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface StateMachineField {

    /**
     * The order of the field to be stored in the snapshot.
     *
     * <p>
     * The fields guarantee the order in which they are stored in the snapshot. This value must be unique across all
     * fields in the state machine. If multiple fields have the same order, an {@link IllegalStateException} will be
     * thrown.
     * </p>
     *
     * @return The order of the field in the snapshot.
     */
    int order();
}
