package org.jgroups.raft.internal.registry;

import org.jgroups.raft.internal.command.JRaftCommand;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

record CommandMetadata(Object stateMachine, long id, int version, Method method, CommandSchema inputSchema, CommandSchema outputSchema) implements ReplicatedMethodWrapper {

    void validate(Method method, JRaftCommand command) {
        if (id != command.id()) {
            throw new IllegalStateException(String.format("Command id mismatch. Expected: %s but got %s", this, command));
        }
        if (version != command.version())
            throw new IllegalStateException(String.format("Command version mismatch. Expected: %s but got %s", this, command));

        // TODO: ensure input and output types match the schema
        /*if (!inputSchema.isTypeAcceptable(command.inputType()))
            throw new IllegalArgumentException(String.format("Command input does not match schema input: %s is not %s", command.inputType(), inputSchema.type()));

        if (!outputSchema().isTypeAcceptable(command.outputType()))
            throw new IllegalArgumentException(String.format("Command output does not match schema input: %s is not %s", command.outputType(), outputSchema.type()));*/
    }

    @Override
    public <O> O submit(Object... input) throws InvocationTargetException, IllegalAccessException {
        Object response = method.invoke(stateMachine, input);

        @SuppressWarnings("unchecked")
        O output = (O) response;
        return output;
    }
}
