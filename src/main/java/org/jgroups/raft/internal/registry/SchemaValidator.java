package org.jgroups.raft.internal.registry;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.util.internal.Json;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Validates the current state machine schema against a previously stored schema to detect backwards-incompatible changes
 * before log replay begins.
 *
 * <p>
 * The validator is invoked during {@link CommandRegistry#evolveSchema(RAFT)} after the Raft log is initialized. On first
 * startup, the current schema is persisted to {@code schema.raft} in the log directory. On subsequent startups, the current
 * schema (derived from annotations) is compared against the stored schema. If validation passes, the merged schema is written back.
 * </p>
 *
 * <h2>Validation Rules</h2>
 * <ul>
 *   <li>The current schema must not contain duplicate {@code (id, version)} entries.</li>
 *   <li>An existing command's {@link SchemaEntry.Kind} and parameter types must not change.</li>
 *   <li>A command can be removed if a higher version of the same id exists (superseded).</li>
 *   <li>A command without a higher version can only be removed if declared as {@link SchemaEntry.Status#RETIRED}.</li>
 *   <li>A retired command must match the stored entry's kind.</li>
 *   <li>A retired command cannot be reactivated.</li>
 *   <li>A command that never existed in the stored schema cannot be retired.</li>
 *   <li>Version downgrades (removing the highest version) are not permitted.</li>
 *   <li>New commands not present in the stored schema are always allowed.</li>
 * </ul>
 *
 * <h2>Schema File Format</h2>
 * <p>
 * The {@code schema.raft} file uses a JSON envelope with a format version for future evolution:
 * </p>
 * <pre>{@code
 * {
 *   "version": 1,
 *   "commands": [
 *     {
 *       "id": 1,
 *       "commandVersion": 1,
 *       "kind": "WRITE",
 *       "parameterTypes": ["java.lang.String"],
 *       "status": "ACTIVE"
 *     }
 *   ]
 * }
 * }</pre>
 *
 * @since 2.0
 * @author Jose Bolina
 * @see SchemaEntry
 * @see CommandRegistry#evolveSchema(RAFT)
 */
final class SchemaValidator {

    private static final Log LOG = LogFactory.getLog(SchemaValidator.class);
    private static final String SCHEMA_FILE = "schema.raft";
    private static final int SCHEMA_FILE_VERSION = 1;

    private SchemaValidator() { }

    /**
     * Validates the current schema against the stored schema in the given directory.
     *
     * <p>
     * If no stored schema exists, the current schema is written and the method returns. If a stored schema exists, the
     * evolution rules are applied. On success, the merged schema (current active entries plus previously stored entries)
     * is written back.
     * </p>
     *
     * @param current   the schema derived from the current state machine annotations
     * @param directory the directory where {@code schema.raft} is stored
     * @throws IllegalStateException if a backwards-incompatible change is detected
     * @throws UncheckedIOException  if reading or writing the schema file fails
     */
    static void validate(Collection<SchemaEntry> current, Path directory) {
        validateNoDuplicates(current);

        SchemaIOHandler io = new SchemaIOHandler(directory);
        if (!io.exists()) {
            LOG.debug("No stored schema found in %s, initializing schema definition", directory);
            io.write(current);
            return;
        }

        Collection<SchemaEntry> previous = io.read();
        SchemaEvolution evolution = new SchemaEvolution(current);
        evolution.verifySafeEvolution(previous);

        Collection<SchemaEntry> updated = evolution.merge(previous);
        io.write(updated);
    }

    private static final class SchemaEvolution {
        private final Collection<SchemaEntry> current;
        private final Map<CommandKey, SchemaEntry> indexed;
        private final Map<Integer, Integer> maxVersions;

        public SchemaEvolution(Collection<SchemaEntry> schema) {
            this.current = schema;
            this.indexed = indexByCommandId(schema);
            this.maxVersions = new HashMap<>();

            for (SchemaEntry entry : schema) {
                maxVersions.merge(entry.id(), entry.version(), Math::max);
            }
        }

        public void verifySafeEvolution(Collection<SchemaEntry> previous) {
            // Validate each stored entry against current.
            // First, we verify the schema evolution is safe.
            for (SchemaEntry stored : previous) {
                CommandKey key = new CommandKey(stored.id(), stored.version());
                SchemaEntry curr = indexed.get(key);

                // If an entry was stored as retired in a previous execution, it must remain retired after restart.
                if (stored.status() == SchemaEntry.Status.RETIRED) {
                    assertEntryRetired(curr);
                    continue;
                }

                // If the command is still defined, it must match the previous definition.
                // We do not allow changing the command kind (read/write), or the method arguments.
                if (curr != null) {
                    assertEntriesMatch(curr, stored);
                    continue;
                }

                // Otherwise, we ensure it is a safe upgrade.
                Integer max = maxVersions.get(stored.id());

                // Verify whether the method was removed and superseded by another with a higher version.
                if (max != null && max > stored.version()) {
                    continue;
                }

                // A method was removed to remain with only a smaller version.
                // This is moving from v2 -> v1.
                if (max != null && max < stored.version()) {
                    String message = String.format("Command (id=%d, version=%d) was removed and remains with command "
                            + "(id=%d, version=%d). Downgrading version is not permitted",
                            stored.id(), stored.version(), stored.id(), max);
                    throw new IllegalStateException(message);
                }

                // A command was removed without replacement or without acknowledgement.
                // We throw the error to notify the user about the removal and they must willingly include the annotation.
                String message = String.format("Command (id=%d, version=%d) was removed without being retired. "
                                + "Include @StateMachine%s(id = %d, version = %d) to the list of retired %s operations in "
                                + "@JGroupsRaftStateMachine. Otherwise, restore the command if it was a mistake",
                        stored.id(), stored.version(), stored.kind() == SchemaEntry.Kind.READ ? "Read" : "Write",
                        stored.id(), stored.version(), stored.kind());
                throw new IllegalStateException(message);
            }

            // Now, we verify the schema evolution is correctly retiring operations.
            // We can only retire operations that were included before.
            Map<CommandKey, SchemaEntry> indexedPrevious = indexByCommandId(previous);
            for (SchemaEntry curr : current) {
                if (curr.status() != SchemaEntry.Status.RETIRED)
                    continue;

                CommandKey key = new CommandKey(curr.id(), curr.version());
                SchemaEntry stored = indexedPrevious.get(key);
                if (stored == null) {
                    String message = String.format("Command (id=%d, version=%d) is retired but it was never registered before. "
                            + "Remove it from the retired list or correct the id/version to the correct command",
                            curr.id(), curr.version());
                    throw new IllegalStateException(message);
                }
            }
        }

        public Collection<SchemaEntry> merge(Collection<SchemaEntry> previous) {
            // Linked map trying to keep some stable ordering in the final list.
            Map<CommandKey, SchemaEntry> merged = new LinkedHashMap<>();

            // First, include the current version of the entries.
            for (SchemaEntry entry : current) {
                CommandKey key = new CommandKey(entry.id(), entry.version());
                merged.put(key, entry);
            }

            // Then include the previous information.
            // This is essential to keep track of removed commands over time.
            // Otherwise, users could start utilizing a previously removed ID.
            for (SchemaEntry entry : previous) {
                CommandKey key = new CommandKey(entry.id(), entry.version());
                merged.putIfAbsent(key, entry);
            }

            return Collections.unmodifiableCollection(merged.values());
        }
    }

    private static final class SchemaIOHandler {
        private final File file;

        public SchemaIOHandler(Path directory) {
            this.file = directory.resolve(SCHEMA_FILE).toFile();
        }

        public boolean exists() {
            return file.exists();
        }

        public void write(Collection<SchemaEntry> schema) {
            // Utilize linked map to keep the order of insertion.
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("version", SCHEMA_FILE_VERSION);

            List<Map<String, Object>> commands = new ArrayList<>();
            for (SchemaEntry entry : schema) {
                Map<String, Object> command = new LinkedHashMap<>();
                command.put("id", entry.id());
                command.put("commandVersion", entry.version());
                command.put("kind", entry.kind().name());
                command.put("parameterTypes", new ArrayList<>(entry.parameters()));
                command.put("status", entry.status().name());
                commands.add(command);
            }

            envelope.put("commands", commands);

            try {
                Path directory = Objects.requireNonNull(file.toPath().getParent(), "The schema file always has a parent");
                if (Files.notExists(directory)) {
                    Files.createDirectories(directory);
                }
                Files.writeString(file.toPath(), Json.toPrettyJson(envelope));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed writing updated schema file: " + file, e);
            }
        }

        public Collection<SchemaEntry> read() {
            String content;
            try {
                content = Files.readString(file.toPath());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed reading schema file: " + file, e);
            }

            Map<String, Object> envelope = Json.fromJson(content);
            Integer version = (Integer) envelope.get("version");
            if (version == null || version > SCHEMA_FILE_VERSION)
                throw new IllegalStateException(String.format("Schema file for version (%s) is not accepted", version));

            List<SchemaEntry> schema = new ArrayList<>();

            if (version == 1) {@SuppressWarnings("unchecked")
                Collection<Object> commands = (Collection<Object>) envelope.get("commands");

                for (Object obj : commands) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> entry = (Map<String, Object>) obj;

                    int id = (Integer) entry.get("id");
                    int commandVersion = (Integer) entry.get("commandVersion");
                    SchemaEntry.Kind kind = SchemaEntry.Kind.valueOf((String) entry.get("kind"));
                    SchemaEntry.Status status = SchemaEntry.Status.valueOf((String) entry.get("status"));

                    @SuppressWarnings("unchecked")
                    Collection<String> parameters = ((Collection<Object>) entry.get("parameterTypes")).stream()
                            .map(Object::toString)
                            .toList();

                    schema.add(new SchemaEntry(id, commandVersion, kind, parameters, status));
                }
            }

            return Collections.unmodifiableCollection(schema);
        }
    }

    private static void validateNoDuplicates(Collection<SchemaEntry> schema) {
        Map<CommandKey, SchemaEntry> seen = new HashMap<>();
        for (SchemaEntry entry : schema) {
            CommandKey key = new CommandKey(entry.id(), entry.version());
            SchemaEntry previous = seen.put(key, entry);
            if (previous != null) {
                String message = String.format("Command (id=%d, version=%d) is declared both as %s and %s. "
                        + "Remove it from either the method annotations or the retired list.", entry.id(), entry.version(), previous.status(), entry.status());
                throw new IllegalStateException(message);
            }
        }
    }

    private static void assertEntryRetired(SchemaEntry entry) {
        if (entry != null && entry.status() != SchemaEntry.Status.RETIRED) {
            String message = String.format("Command (id=%d, version=%d) was previously retired and cannot be reactivated. "
                    + "Utilize a new ID or increase the version for the new command", entry.id(), entry.version());
            throw new IllegalStateException(message);
        }
    }

    private static void assertEntriesMatch(SchemaEntry current, SchemaEntry previous) {
        if (current.status() == SchemaEntry.Status.RETIRED) {
            if (previous.kind() != current.kind()) {
                String message = String.format("Command (id=%d, version=%d) changed from a %s operation to %s when it was retired. "
                                + "The retired annotation must utilize the same kind as the original command to ensure a safe upgrade",
                        current.id(), current.version(), previous.kind(), current.kind());
                throw new IllegalStateException(message);
            }
            return;
        }

        if (previous.kind() != current.kind()) {
            String message = String.format("Command (id=%d, version=%d) changed from %s to %s between restarts. "
                            + "You should retire the old method and introduce a new definition with higher version",
                    current.id(), current.version(), previous.kind(), current.kind());
            throw new IllegalStateException(message);
        }

        if (!previous.parameters().equals(current.parameters())) {
            String message = String.format("Command (id=%d, version=%d) parameter types changed between restarts. "
                            + "It was '%s' and now is '%s'. You should retire the old method and introduce a new definition with a higher version",
                    current.id(), current.version(), previous.parameters(), current.parameters());
            throw new IllegalStateException(message);
        }
    }

    private static Map<CommandKey, SchemaEntry> indexByCommandId(Collection<SchemaEntry> schema) {
        Map<CommandKey, SchemaEntry> index = new HashMap<>();
        for (SchemaEntry entry : schema) {
            CommandKey key = new CommandKey(entry.id(), entry.version());
            index.put(key, entry);
        }

        return Collections.unmodifiableMap(index);
    }
}
