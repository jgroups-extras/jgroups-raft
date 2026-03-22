package org.jgroups.raft.internal.statemachine;

import org.jgroups.raft.Settable;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.command.JGroupsRaftWriteCommandOptions;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.serialization.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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

    private ProxyWrapper<T> proxy;

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
        StateMachineInvocationHandler<T> handler = new StateMachineInvocationHandler<>(concrete, registry, serializer, settable);
        this.proxy = new ProxyWrapper<>(api, handler);
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
        return proxy.submit(function, options);
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

    private static final class ProxyWrapper<T> {
        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(Object[].class);

        private final StateMachineInvocationHandler<T> handler;
        private final T direct;
        private final Class<T> api;

        // Pre-builds one proxy per boolean option combination and selects via array index, eliminating some more expensive
        // synchronization mechanisms like ThreadLocal or ConcurrentHashMap. The index is computed by bit-packing the boolean
        // fields in the command options (e.g., linearizable | ignoreReturnValue -> 2-bit index into reads[4]).
        //
        // This scheme only works while command options are exclusively boolean.
        // If a non-boolean option is added, this must be replaced with a different caching strategy.
        // See ProxyWrapperArraySizeTest which guards this assumption.
        private final T[] writes;
        private final T[] reads;

        @SuppressWarnings("unchecked")
        public ProxyWrapper(Class<T> api, StateMachineInvocationHandler<T> handler) {
            this.handler = handler;
            this.direct = (T) Proxy.newProxyInstance(
                    api.getClassLoader(),
                    new Class<?>[]{ api },
                    handler
            );
            this.api = api;
            this.writes = (T[]) new Object[2];
            this.reads = (T[]) new Object[4];
        }

        @SuppressWarnings("unchecked")
        private T createIndirectProxy(JGroupsRaftCommandOptions options) {
            InvocationHandler ih = (proxy, method, args) -> handler.invoke(proxy, method, args, options);
            return (T) Proxy.newProxyInstance(api.getClassLoader(), new Class<?>[] { api }, ih);
        }

        public <O> O submit(Function<T, O> function, JGroupsRaftCommandOptions options) {
            if (options == null) {
                return function.apply(direct);
            }

            if (options instanceof JGroupsRaftWriteCommandOptions wco) {
                return submitWrite(function, wco);
            }

            if (options instanceof JGroupsRaftReadCommandOptions rco) {
                return submitRead(function, rco);
            }

            throw new IllegalStateException("Unknown command option type: " + options);
        }

        private <O> O submitWrite(Function<T, O> function, JGroupsRaftWriteCommandOptions options) {
            int index = options.ignoreReturnValue() ? 1 : 0;
            return function.apply(acquire(index, writes, options));
        }

        private <O> O submitRead(Function<T, O> function, JGroupsRaftReadCommandOptions options) {
            int index = (options.linearizable() ? 2 : 0) | (options.ignoreReturnValue() ? 1 : 0);
            return function.apply(acquire(index, reads, options));
        }

        private T acquire(int index, T[] arr, JGroupsRaftCommandOptions options) {
            T t = get(arr, index);
            if (t == null) {
                synchronized (arr) {
                    t = get(arr, index);
                    if (t == null) {
                        t = createIndirectProxy(options);
                        ARRAY_HANDLE.setRelease(arr, index, t);
                    }
                }
            }

            return t;
        }

        @SuppressWarnings("unchecked")
        private static <T> T get(T[] arr, int index) {
            return (T) ARRAY_HANDLE.getAcquire(arr, index);
        }
    }
}
