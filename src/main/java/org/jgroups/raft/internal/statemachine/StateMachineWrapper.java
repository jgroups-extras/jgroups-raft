package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.Settable;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.serialization.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.function.Function;

/**
 * A facade and lifecycle manager that bridges the JGroups Raft {@link StateMachine} with the user-defined state machine API.
 *
 * <p>
 * This wrapper serves two primary purposes:
 * <ol>
 *   <li><b>Client-Facing Proxy:</b> It generates a dynamic proxy implementing the user's API interface. When the user
 *       invokes a method on this proxy, the call is intercepted and submitted to the Raft cluster.</li>
 *   <li><b>Raft Callback Target:</b> It implements JGroups' {@link StateMachine}, receiving committed log entries from
 *       the cluster and delegating them to the {@link StateMachineAdapter} for local application.</li>
 * </ol>
 *
 * @since 2.0
 * @author José Bolina
 * @see StateMachine
 * @param <T> The type of the user-defined state machine API interface.
 */
public final class StateMachineWrapper<T> implements StateMachine {

    private final T concrete;
    private final Class<T> api;
    private final CommandRegistry<T> registry;
    private final Serializer serializer;
    private final StateMachine delegate;

    private Settable settable;
    private WrapperType type;
    private T wrapper;

    /**
     * Constructs the wrapper, initializing the internal adapter for log application.
     *
     * @param concrete   The concrete, user-provided implementation of the state machine.
     * @param api        The interface defining the state machine's generic API.
     * @param registry   The registry containing parsed metadata and schemas for the API's commands.
     * @param serializer The serializer used to convert commands to/from byte arrays for Raft replication.
     */
    public StateMachineWrapper(T concrete, Class<T> api, CommandRegistry<T> registry, Serializer serializer) {
        this.concrete = concrete;
        this.api = api;
        this.delegate = new StateMachineAdapter<>(serializer, registry, concrete);
        this.registry = registry;
        this.serializer = serializer;
    }

    /**
     * Initializes the client-facing proxy and binds the wrapper to the Raft cluster's {@link Settable}.
     *
     * <p>
     * Future extensions may utilize generated code instead of standard Java Reflection Proxies for improved performance.
     * </p>
     *
     * @param settable The Raft cluster component used to asynchronously submit requests (get/set).
     */
    public void initialize(Settable settable) {
        this.settable = settable;

        if (verifyGenerateClasses()) {
            this.type = WrapperType.GENERATED;
            this.wrapper = useGeneratedSources();
            return;
        }

        this.type = WrapperType.PROXY;
        this.wrapper = useProxy(null);
    }

    /**
     * Executes a single function against the state machine proxy, applying specific command options to that invocation
     * (e.g., overriding read linearizability or return value requirements).
     *
     * @param function The functional invocation to perform on the state machine proxy.
     * @param options  The runtime options to apply to this specific execution.
     * @param <O>      The expected return type.
     * @return The result of the state machine execution.
     */
    public <O> O submit(Function<T, O> function, JGroupsRaftCommandOptions options) {
        if (options == null)
            return function.apply(wrapper);

        if (type == WrapperType.GENERATED) {
            return submitGeneratedWithOptions();
        }

        return submitProxyWithOptions(wrapper, function, options);
    }

    /**
     * Creates a new, specialized proxy wrapper that bakes in default execution options or restricts execution to methods
     * tagged with a specific annotation.
     *
     * @param options    The default options to apply to all invocations through this new proxy.
     * @param restricted An optional annotation class. If provided, the proxy will strictly reject any methods not
     *                   annotated with this type.
     * @return A customized proxy instance.
     */
    public T createWrapper(JGroupsRaftCommandOptions options, Class<? extends Annotation> restricted) {
        if (type == WrapperType.GENERATED) {
            //T dispatcher = useGeneratedSources();
            return createGeneratedWithOptions();
        }

        T dispatcher = useProxy(restricted);
        return createProxyWithOptions(dispatcher, options);
    }

    private static <T> boolean verifyGenerateClasses() {
        return false;
    }

    private T useGeneratedSources() {
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

    private <O> O submitGeneratedWithOptions() {
        // Implementation for generated sources would go here
        throw new UnsupportedOperationException();
    }

    private T createGeneratedWithOptions() {
        throw new UnsupportedOperationException();
    }

    /**
     * Delegate invocation of state machine apply method.
     *
     * @param data The byte[] buffer
     * @param offset The offset at which the data starts
     * @param length The length of the data
     * @param serialize_response If true, serialize and return the response, else return null
     * @return The state machine response.
     */
    @Override
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
        return delegate.apply(data, offset, length, serialize_response);
    }

    @Override
    public void readContentFrom(DataInput in) {
        delegate.readContentFrom(in);
    }

    @Override
    public void writeContentTo(DataOutput out) throws Exception {
        delegate.writeContentTo(out);
    }

    /**
     * The mode to wrap the state machine implementation.
     *
     * <p>
     * We only support {@link WrapperType#PROXY} at the moment.
     * </p>
     */
    private enum WrapperType {
        /**
         * Utilize the Java {@link Proxy} API to redirect the calls to Raft and delegate the state machine.
         */
        PROXY,

        /**
         * Utilize code generated during compile-time to wrap the concrete state machine implementation.
         */
        GENERATED;
    }
}
