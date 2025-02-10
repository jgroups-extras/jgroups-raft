package org.jgroups.raft.internal.registry;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.jgroups.raft.internal.command.JRaftCommand;

record CommandMetadata(long id, int version, Method method, CommandSchema inputSchema, CommandSchema outputSchema) {

    <O> O submit(Object sm, Object[] input) throws InvocationTargetException, IllegalAccessException {
        Object response = method.invoke(sm, input);

        if (response == null) return null;

        if (!outputSchema.isAcceptable(response))
            throw new IllegalStateException("Invalid output type: " + response.getClass());

        @SuppressWarnings("unchecked")
        O output = (O) response;
        return output;
    }

    void validate(Method method, JRaftCommand command) {
        if (version != command.version())
            throw new IllegalArgumentException(String.format("Command version does not match: %d is not %d", command.version(), version));

        // TODO: ensure input and output types match the schema
        /*if (!inputSchema.isTypeAcceptable(command.inputType()))
            throw new IllegalArgumentException(String.format("Command input does not match schema input: %s is not %s", command.inputType(), inputSchema.type()));

        if (!outputSchema().isTypeAcceptable(command.outputType()))
            throw new IllegalArgumentException(String.format("Command output does not match schema input: %s is not %s", command.outputType(), outputSchema.type()));*/
    }
}
