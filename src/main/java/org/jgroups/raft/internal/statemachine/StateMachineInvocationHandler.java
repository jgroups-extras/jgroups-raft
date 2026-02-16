package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.Settable;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.exceptions.JRaftException;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.RaftCommand;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.registry.ReplicatedMethodWrapper;
import org.jgroups.raft.internal.serialization.Serializer;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Intercepts method invocations on the user's proxy interface and translates them into replicated Raft commands.
 *
 * <p>
 * This handler evaluates annotations to determine the nature of the request:
 * <ul>
 *   <li><b>Unannotated Methods:</b> Executed locally on the concrete instance, bypassing Raft. These methods are outside
 *       the state machine contract.</li>
 *   <li><b>Non-Linearizable Reads:</b> Executed locally for immediate, potentially stale reads.</li>
 *   <li><b>Replicated Commands:</b> Serialized and submitted to the Raft cluster for consensus.</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @param <T> The type of the user-defined state machine interface.
 */
final class StateMachineInvocationHandler<T> implements InvocationHandler {

    private final T delegate;
    private final CommandRegistry<T> registry;
    private final Serializer serializer;
    private final Settable settable;
    private final Class<? extends Annotation> permitted;

    StateMachineInvocationHandler(T delegate, CommandRegistry<T> registry, Serializer serializer, Settable settable, Class<? extends Annotation> permitted) {
        this.delegate = delegate;
        this.registry = registry;
        this.serializer = serializer;
        this.settable = settable;
        this.permitted = permitted;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws InvocationTargetException, IllegalAccessException {
        return invoke(proxy, method, args, null);
    }

    /**
     * Core interception logic for the dynamic proxy.
     *
     * @param ignore  The proxy instance (unused).
     * @param method  The method invoked by the user.
     * @param args    The arguments passed to the method.
     * @param options Execution-specific options (e.g., linearizability overrides).
     * @return The result of the method execution (or a Future if async).
     */
    public Object invoke(Object ignore, Method method, Object[] args, JGroupsRaftCommandOptions options) throws InvocationTargetException, IllegalAccessException {
        JRaftCommand command;

        // 1. Enforce annotation restrictions (if applicable)
        // For example, the user is utilizing a JGroupsRaft instance for read-only operations.
        // We restrict the allowed operations to read operations only, in this example.
        if (permitted != null) {
            command = findPermittedMethod(method);

            if (command == null && searchMethod(method) != null)
                throw new JRaftException(String.format("Only methods annotated with %s are allowed", permitted));
        } else {
            command = searchMethod(method);
        }

        // 2. Unannotated methods bypass the Raft protocol completely
        // We assume these are methods outside the state machine contract. Otherwise, they would be annotated.
        if (command == null)
            return method.invoke(delegate, args);

        RaftCommand wrapper = new RaftCommand(command, args, options);

        // 3. Handle Non-Linearizable (Dirty) Reads
        // If a read does not require strict Raft consensus, execute it immediately on the local state machine.
        if (wrapper.isRead()) {
            if (options instanceof JGroupsRaftReadCommandOptions opts && !opts.linearizable()) {
                ReplicatedMethodWrapper<?> rmw = registry.getCommand(command.id());
                Object res;

                // CRITICAL: Synchronize on the concrete implementation's intrinsic lock.
                // This prevents dirty reads from colliding with the StateMachineAdapter which is concurrently applying
                // committed writes to the same instance.
                synchronized (delegate) {
                    res = rmw.submit(args);
                }
                return opts.ignoreReturnValue() ? null : res;
            }
        }

        // 4. Submit to Raft Cluster
        // This will replicate the entry through the cluster and apply to each state machine.
        boolean isAsync = isAsync(method);
        CompletableFuture<Object> cf = submit(wrapper, options);

        // Return the Future directly for async methods, otherwise block and wait for consensus.
        return isAsync ? cf : cf.join();
    }

    private JRaftCommand searchMethod(Method method) {
        if (method.getAnnotation(StateMachineWrite.class) != null) {
            return registry.getCommand(method);
        }

        if (method.getAnnotation(StateMachineRead.class) != null) {
            return registry.getCommand(method);
        }

        return null;
    }

    private JRaftCommand findPermittedMethod(Method method) {
        if (method.getAnnotation(permitted) == null) {
            return null;
        }
        return registry.getCommand(method);
    }

    private boolean isAsync(Method method) {
        Class<?> clazz = method.getReturnType();
        return Future.class.isAssignableFrom(clazz)
                || CompletionStage.class.isAssignableFrom(clazz)
                || CompletableFuture.class.isAssignableFrom(clazz);
    }

    /**
     * Serializes the command payload and dispatches it to the Raft cluster via {@link Settable}.
     *
     * @param command The command to submit.
     * @param options The command options to customize the command behavior.
     * @param <O> The response type.
     * @return A future that completes once the command is committed. The response follows the definition in the options.
     */
    private <O> CompletableFuture<O> submit(RaftCommand command, JGroupsRaftCommandOptions options) {
        boolean deserialize = options == null || !options.ignoreReturnValue();
        Function<byte[], O> mapper = deserialize
                ? serializer::deserialize
                : StateMachineInvocationHandler::toNull;
        byte[] buf = serializer.serialize(command);

        try {
            CompletableFuture<byte[]> cs;
            if (command.isRead()) {
                cs = settable.getAsync(buf, 0, buf.length);
            } else {
                cs = settable.setAsync(buf, 0, buf.length);
            }

            return cs.thenApply(mapper);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static <R> R toNull(Object ignore) {
        return null;
    }
}
