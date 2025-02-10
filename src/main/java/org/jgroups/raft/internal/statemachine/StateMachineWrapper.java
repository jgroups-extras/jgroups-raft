package org.jgroups.raft.internal.statemachine;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.function.Function;

import org.jgroups.raft.Settable;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.serialization.Serializer;

public final class StateMachineWrapper<T> implements StateMachine {

    private final T concrete;
    private final Class<T> api;
    private final CommandRegistry<T> registry;
    private final Serializer serializer;
    private final StateMachine delegate;

    private Settable settable;
    private WrapperType type;
    private T wrapper;

    public StateMachineWrapper(T concrete, Class<T> api, CommandRegistry<T> registry, Serializer serializer) {
        this.concrete = concrete;
        this.api = api;
        this.delegate = new StateMachineAdapter<>(serializer, registry, concrete);
        this.registry = registry;
        this.serializer = serializer;
    }

    public void initialize(Settable settable) {
        this.settable = settable;

        if (verifyGenerateClasses(api)) {
            this.type = WrapperType.GENERATED;
            this.wrapper = useGeneratedSources(null);
            return;
        }

        this.type = WrapperType.PROXY;
        this.wrapper = useProxy(null);
    }

    public <O> O submit(Function<T, O> function, JGroupsRaftCommandOptions options) {
        if (options == null)
            return function.apply(wrapper);

        if (type == WrapperType.GENERATED) {
            return submitGeneratedWithOptions(function, options);
        }

        return submitProxyWithOptions(wrapper, function, options);
    }

    public T createWrapper(JGroupsRaftCommandOptions options, Class<? extends Annotation> restricted) {
        if (type == WrapperType.GENERATED) {
            T dispatcher = useGeneratedSources(restricted);
            return createGeneratedWithOptions(dispatcher, options);
        }

        T dispatcher = useProxy(restricted);
        return createProxyWithOptions(dispatcher, options);
    }

    private static <T> boolean verifyGenerateClasses(Class<T> api) {
        return false;
    }

    private T useGeneratedSources(Class<? extends Annotation> restricted) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    private T useProxy(Class<? extends Annotation> restricted) {
        StateMachineInvocationHandler<T> handler = new StateMachineInvocationHandler<>(concrete, registry, serializer, settable, restricted);
        return (T) Proxy.newProxyInstance(
                api.getClassLoader(),
                new Class<?>[]{ api },
                handler
        );
    }

    private <O> O submitProxyWithOptions(T dispatcher, Function<T, O> function, JGroupsRaftCommandOptions options) {
        T sm = createProxyWithOptions(dispatcher, options);
        return function.apply(sm);
    }

    @SuppressWarnings("unchecked")
    private T createProxyWithOptions(T dispatcher, JGroupsRaftCommandOptions options) {
        InvocationHandler ih = (proxy, method, args) -> {
            StateMachineInvocationHandler<T> handler = (StateMachineInvocationHandler<T>) Proxy.getInvocationHandler(dispatcher);
            return handler.invoke(proxy, method, args, options);
        };
        return (T) Proxy.newProxyInstance(
              api.getClassLoader(),
              new Class<?>[]{ api },
              ih
        );
    }

    private <O> O submitGeneratedWithOptions(Function<T, O> function, JGroupsRaftCommandOptions options) {
        // Implementation for generated sources would go here
        throw new UnsupportedOperationException();
    }

    private T createGeneratedWithOptions(T dispatcher, JGroupsRaftCommandOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
        return delegate.apply(data, offset, length, serialize_response);
    }

    @Override
    public void readContentFrom(DataInput in) throws Exception {
        delegate.readContentFrom(in);
    }

    @Override
    public void writeContentTo(DataOutput out) throws Exception {
        delegate.writeContentTo(out);
    }

    private enum WrapperType {
        PROXY,
        GENERATED;
    }
}
