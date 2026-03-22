package org.jgroups.raft.internal.registry;

import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.JRaftReadCommand;
import org.jgroups.raft.internal.command.JRaftWriteCommand;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of commands that can be executed on a {@link StateMachine}.
 *
 * <p>
 * The registry uses reflection to discover methods annotated with {@link StateMachineRead} and {@link StateMachineWrite}
 * on the state machine interface. Each command is identified by a composite {@link CommandKey} of {@code (id, version)},
 * enabling multiple versions of the same command to coexist for backwards-compatible log replay.
 * </p>
 *
 * <p>
 * After initialization, {@link #evolveSchema(RAFT)} compares the current command schema against a previously stored version
 * in {@code schema.raft}, detecting backwards-incompatible changes before log replay begins. Validation is performed by
 * {@link SchemaValidator}.
 * </p>
 *
 * @param <T> the state machine type
 * @since 2.0
 * @author Jose Bolina
 * @see SchemaValidator
 * @see SchemaEntry
 **/
public class CommandRegistry<T> {
    private final Map<CommandKey, CommandMetadata> registry = new HashMap<>();
    private final Map<Method, JRaftCommand> commands = new ConcurrentHashMap<>();
    private final T stateMachine;
    private final Class<T> api;

    public CommandRegistry(T stateMachine, Class<T> api) {
        this.api = api;
        this.stateMachine = stateMachine;
    }

    public void initialize() {
        Class<?> clazz = api;
        if (clazz.getAnnotation(JGroupsRaftStateMachine.class) == null)
            throw new IllegalStateException("State machine class must be annotated with @JGroupsRaftStateMachine");

        List<Method> methods = new ArrayList<>();
        methods.addAll(List.of(clazz.getDeclaredMethods()));
        methods.addAll(List.of(stateMachine.getClass().getDeclaredMethods()));

        for (Method method : methods) {
            int id = 0;
            int version = 0;

            Annotation annotation = null;
            StateMachineWrite write = method.getAnnotation(StateMachineWrite.class);
            if (write != null) {
                JRaftCommand previous = commands.put(method, createWriteCommand(write));
                if (previous != null)
                    throw new IllegalStateException("Command " + method.getName() + " is already registered for writes");

                id = write.id();
                version = write.version();
                annotation = write;
            }

            StateMachineRead read = method.getAnnotation(StateMachineRead.class);
            if (read != null) {
                JRaftCommand previous = commands.put(method, createReadCommand(read));
                if (previous != null)
                    throw new IllegalStateException("Command " + method.getName() + " is already registered for reads");

                id = read.id();
                version = read.version();
                annotation = read;
            }

            if (annotation != null) {
                CommandMetadata metadata = createCommandMetadata(id, version, method, annotation);
                CommandKey key = new CommandKey(id, version);
                CommandMetadata other = registry.put(key, metadata);
                if (other != null) {
                    String message = String.format("Found duplicated method '%s' and '%s' both with (id = %d, version = %d)", metadata.method().getName(), other.method().getName(), id, version);
                    throw new IllegalStateException(message);
                }

                JRaftCommand command = commands.get(method);
                metadata.validate(method, command);
            }
        }
    }

    /**
     * Validates the current state machine schema against the stored schema in the Raft log directory.
     *
     * <p>
     * This method must be called before the {@link RAFT} protocol is initialized (i.e., before the channel is connected).
     * If the log is not a {@link FileBasedLog}, validation is skipped since in-memory logs do not persist across restarts.
     * </p>
     *
     * <p>
     * On first startup, the schema is written to a file. On subsequent startups, the current schema is compared against
     * the stored version.
     * </p>
     *
     * @param raft the RAFT protocol instance, or {@code null} to skip validation
     * @throws IllegalStateException if a backwards-incompatible schema change is detected
     * @see SchemaValidator
     */
    public void evolveSchema(RAFT raft) {
        if (raft == null || raft.logClass() == null)
            return;

        if (!Objects.equals(FileBasedLog.class.getName(), raft.logClass()))
            return;

        String base = raft.logDir();
        String prefix = raft.logPrefix() == null
                ? raft.raftId()
                : raft.logPrefix();
        Path path = Path.of(base, prefix + ".log");
        JGroupsRaftStateMachine ann = api.getAnnotation(JGroupsRaftStateMachine.class);
        Collection<SchemaEntry> schema = SchemaEntry.create(ann, registry.values());
        SchemaValidator.validate(schema, path);
    }

    public JRaftCommand getCommand(Method method) {
        return commands.get(method);
    }

    public ReplicatedMethodWrapper getCommand(int id, int version) {
        CommandMetadata metadata = registry.get(new CommandKey(id, version));
        if (metadata == null)
            throw new IllegalStateException(String.format("Command (id=%d, version=%s) not found in registry", id, version));

        return metadata;
    }

    private JRaftCommand createWriteCommand(StateMachineWrite write) {
        return JRaftWriteCommand.create(write.id(), write.version());
    }

    private JRaftCommand createReadCommand(StateMachineRead read) {
        return JRaftReadCommand.create(read.id(), read.version());
    }

    private CommandMetadata createCommandMetadata(int id, int version, Method method, Annotation annotation) {
        if (!method.canAccess(stateMachine)) {
            method.setAccessible(true);
        }

        Type outputType = method.getGenericReturnType();
        Collection<CommandSchema> inputs = Arrays.stream(method.getGenericParameterTypes())
                .map(this::createCommandSchema)
                .toList();
        return new CommandMetadata(stateMachine, id, version, annotation, method, inputs, createCommandSchema(outputType));
    }

    private CommandSchema createCommandSchema(Type type) {
        return new CommandSchema(type);
    }
}
