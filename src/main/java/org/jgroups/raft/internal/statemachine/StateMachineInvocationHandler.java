package org.jgroups.raft.internal.statemachine;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import org.jgroups.raft.Settable;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.exceptions.JRaftException;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.RaftCommand;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.serialization.Serializer;

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

    public Object invoke(Object ignore, Method method, Object[] args, JGroupsRaftCommandOptions options) throws InvocationTargetException, IllegalAccessException {
        JRaftCommand command = null;
        if (permitted != null) {
            command = findPermittedMethod(method);

            if (command == null && searchMethod(method) != null)
                throw new JRaftException(String.format("Only methods annotated with %s are allowed", permitted));
        } else {
            command = searchMethod(method);
        }

        if (command == null)
            return method.invoke(delegate, args);

        boolean isAsync = isAsync(method);
        CompletableFuture<Object> cf = submit(new RaftCommand(command, args, options));
        return isAsync
                ? cf
                : cf.join();
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

    private <O> CompletableFuture<O> submit(RaftCommand command) {
        byte[] buf = serializer.serialize(command);

        try {
            return settable.setAsync(buf, 0, buf.length)
                    .thenApply(serializer::deserialize);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
