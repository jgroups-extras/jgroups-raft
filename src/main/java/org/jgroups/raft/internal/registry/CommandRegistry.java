package org.jgroups.raft.internal.registry;

import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.JRaftReadCommand;
import org.jgroups.raft.internal.command.JRaftWriteCommand;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of commands that can be executed on a {@link StateMachine}.
 *
 * // Explain how it works by reflection and looking for the annotation.
 * // Explain how it validates the state machine schema generated during runtime.
 *
 * @param <T>
 * @version 2.0
 * @author José Bolina
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
            boolean registerMethod = false;
            int id = 0;
            int version = 0;

            StateMachineWrite write = method.getAnnotation(StateMachineWrite.class);
            if (write != null) {
                JRaftCommand previous = commands.put(method, createWriteCommand(write));
                if (previous != null)
                    throw new IllegalStateException("Command " + method.getName() + " is already registered for writes");

                id = write.id();
                version = write.version();
                registerMethod = true;
            }

            StateMachineRead read = method.getAnnotation(StateMachineRead.class);
            if (read != null) {
                JRaftCommand previous = commands.put(method, createReadCommand(read));
                if (previous != null)
                    throw new IllegalStateException("Command " + method.getName() + " is already registered for reads");

                id = read.id();
                version = read.version();
                registerMethod = true;
            }

            if (registerMethod) {
                CommandMetadata metadata = createCommandMetadata(method, id, version);
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

    public JRaftCommand getCommand(Method method) {
        JRaftCommand command = commands.get(method);
        if (command == null) {
            throw new IllegalStateException("Method " + method.getName() + " is not registered as a command");
        }
        return command;
    }

    public void destroy() {
        // Flush the current schema to log. This is utilized for validation at restart.
        // TODO: write to log.

        // Clear all the in-memory registrations.
        registry.clear();
    }

    public void validateCommand(JRaftCommand command) {
        CommandMetadata metadata = registry.get(new CommandKey(command.id(), command.version()));
        if (metadata == null)
            throw new IllegalArgumentException("Unknown command id: " + command.id());

        metadata.validate(null, command);
    }

    public ReplicatedMethodWrapper getCommand(int id, int version) {
        CommandMetadata metadata = registry.get(new CommandKey(id, version));
        if (metadata == null)
            throw new IllegalStateException("Unknown command id: " + id);

        return metadata;
    }

    private JRaftCommand createWriteCommand(StateMachineWrite write) {
        return JRaftWriteCommand.create(write.id(), write.version());
    }

    private JRaftCommand createReadCommand(StateMachineRead read) {
        return JRaftReadCommand.create(read.id(), read.version());
    }

    private CommandMetadata createCommandMetadata(Method method, int id, int version) {
        if (!method.canAccess(stateMachine)) {
            method.setAccessible(true);
        }

        Type outputType = method.getGenericReturnType();
        Collection<CommandSchema> inputs = Arrays.stream(method.getGenericParameterTypes())
                .map(this::createCommandSchema)
                .toList();
        return new CommandMetadata(stateMachine, id, version, method, inputs, createCommandSchema(outputType));
    }

    private CommandSchema createCommandSchema(Type type) {
        return new CommandSchema(type);
    }
}
