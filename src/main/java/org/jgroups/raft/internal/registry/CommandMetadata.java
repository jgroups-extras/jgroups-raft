package org.jgroups.raft.internal.registry;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Objects;

final class CommandMetadata implements ReplicatedMethodWrapper {
    private final Object stateMachine;
    private final int id;
    private final int version;
    private final Annotation annotation;
    private final Method method;
    private final Collection<CommandSchema> inputSchema;
    private final CommandSchema outputSchema;

    private final MethodHandle handle;

    CommandMetadata(
            Object stateMachine,
            int id,
            int version,
            Annotation annotation,
            Method method,
            Collection<CommandSchema> inputSchema,
            CommandSchema outputSchema
    ) {
        this.stateMachine = stateMachine;
        this.id = id;
        this.version = version;
        this.annotation = annotation;
        this.method = method;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;

        // Pre-compute a MethodHandle bound to the state machine instance for direct invocation, avoiding per-call security
        // checks that Method.invoke() performs.
        //
        // The pipeline: unreflect -> bindTo -> asSpreader -> asType
        //   - unreflect:  converts the reflected Method into a MethodHandle
        //   - bindTo:     binds the state machine instance as the receiver (first argument)
        //   - asSpreader: pre-configures Object[] unpacking so invoke doesn't rebuild a spreader each call
        //   - asType:     normalizes the signature to (Object[])Object, enabling invokeExact without per-call type
        //                 adaptation (avoids Invokers.checkGenericType overhead)
        try {
            MethodHandle mh = MethodHandles.privateLookupIn(method.getDeclaringClass(), MethodHandles.lookup())
                    .unreflect(method).bindTo(stateMachine);
            MethodHandle spreader = mh.asSpreader(Object[].class, method.getParameterCount());
            this.handle = spreader.asType(MethodType.methodType(Object.class, Object[].class));
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(String.format("Failed binding state machine method '%s' with (id=%d, version=%d)", method.getName(), id, version), e);
        }
    }

    @Override
    public <O> O submit(Object... input) throws InvocationTargetException, IllegalAccessException {
        try {
            // invokeExact requires an exact type match at the call site, so we can skip any extra validation. The (Object)
            // cast is not redundant, it's a compiler hint that tells invokeExact the expected return type, matching the
            // (Object[])Object signature we normalized to in the constructor via asType().
            Object response = (Object) handle.invokeExact(input);

            @SuppressWarnings("unchecked")
            O output = (O) response;
            return output;
        } catch (Throwable t) {
            if (t instanceof InvocationTargetException ite) throw ite;
            if (t instanceof IllegalAccessException iae) throw iae;
            throw new InvocationTargetException(t, "Failed invoking state machine method");
        }
    }

    public int id() {
        return id;
    }

    public int version() {
        return version;
    }

    public Annotation annotation() {
        return annotation;
    }

    public Method method() {
        return method;
    }

    public Collection<CommandSchema> inputSchema() {
        return inputSchema;
    }

    public CommandSchema outputSchema() {
        return outputSchema;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (CommandMetadata) obj;
        return Objects.equals(this.stateMachine, that.stateMachine) &&
                this.id == that.id &&
                this.version == that.version &&
                Objects.equals(this.annotation, that.annotation) &&
                Objects.equals(this.method, that.method) &&
                Objects.equals(this.inputSchema, that.inputSchema) &&
                Objects.equals(this.outputSchema, that.outputSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateMachine, id, version, annotation, method, inputSchema, outputSchema);
    }

    @Override
    public String toString() {
        return "CommandMetadata[" +
                "stateMachine=" + stateMachine + ", " +
                "id=" + id + ", " +
                "version=" + version + ", " +
                "annotation=" + annotation + ", " +
                "method=" + method + ", " +
                "inputSchema=" + inputSchema + ", " +
                "outputSchema=" + outputSchema + ']';
    }
}
