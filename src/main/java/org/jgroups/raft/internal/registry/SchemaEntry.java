package org.jgroups.raft.internal.registry;

import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Describes a single command's shape for schema validation and persistence.
 *
 * <p>
 * A schema entry captures the identity and signature of a state machine command at a point in time. The {@link SchemaValidator}
 * compares the current set of entries (derived from annotations) against a previously stored set (from {@code schema.raft})
 * to detect backwards-incompatible changes before log replay begins.
 * </p>
 *
 * <p>
 * Entries are either {@link Status#ACTIVE} (derived from annotated methods) or {@link Status#RETIRED} (declared via
 * {@link JGroupsRaftStateMachine#retiredReads()} / {@link JGroupsRaftStateMachine#retiredWrites()}). Retired entries have
 * empty {@code parameters} since the method no longer exists.
 * </p>
 *
 * @param id         the command identifier, matching {@link StateMachineRead#id()} or {@link StateMachineWrite#id()}
 * @param version    the command version
 * @param kind       whether this command is a read or write operation
 * @param parameters fully qualified type names of the method parameters, in declaration order
 * @param status     whether this command is active or has been retired
 * @since 2.0
 * @author José Bolina
 */
record SchemaEntry(
        int id,
        int version,
        Kind kind,
        Collection<String> parameters,
        Status status
) {
    /**
     * Whether the command modifies state ({@code WRITE}) or only queries it ({@code READ}).
     */
    enum Kind {
        READ,
        WRITE
    }

    /**
     * Whether the command is currently implemented ({@code ACTIVE}) or has been permanently removed from the state machine
     * interface ({@code RETIRED}).
     */
    enum Status {
        ACTIVE,
        RETIRED
    }

    private static SchemaEntry create(CommandMetadata metadata) {
        Kind kind = metadata.annotation() instanceof StateMachineRead
                ? Kind.READ
                : Kind.WRITE;
        Collection<String> parameters = metadata.inputSchema().stream()
                .map(cs -> cs.type().getTypeName())
                .toList();

        return new SchemaEntry(metadata.id(), metadata.version(), kind, parameters, Status.ACTIVE);
    }

    /**
     * Builds the complete schema for the current state machine, combining active commands from the registry with retired
     * commands declared in the annotation.
     *
     * @param annotation the {@link JGroupsRaftStateMachine} annotation containing retired declarations
     * @param commands   the active command metadata from the registry
     * @return an unmodifiable list of all schema entries
     */
    static List<SchemaEntry> create(JGroupsRaftStateMachine annotation, Collection<CommandMetadata> commands) {
        List<SchemaEntry> entries = new ArrayList<>();
        for (CommandMetadata command : commands) {
            entries.add(create(command));
        }

        for (StateMachineRead read : annotation.retiredReads()) {
            entries.add(new SchemaEntry(read.id(), read.version(), Kind.READ, List.of(), Status.RETIRED));
        }

        for (StateMachineWrite write : annotation.retiredWrites()) {
            entries.add(new SchemaEntry(write.id(), write.version(), Kind.WRITE, List.of(), Status.RETIRED));
        }

        return Collections.unmodifiableList(entries);
    }
}
