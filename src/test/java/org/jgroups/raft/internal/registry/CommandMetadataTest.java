package org.jgroups.raft.internal.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.JRaftReadCommand;
import org.jgroups.raft.internal.command.JRaftWriteCommand;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CommandMetadataTest {

    // Dummy target to provide reliable reflection signatures for the metadata testing
    static class MetadataTarget {
        public String singleArg(String a) {
            return a;
        }

        public int primitiveArg(int a) {
            return a + 1;
        }

        public void noArg() {}

        public <T extends Number> T genericReturn(T val) {
            return val;
        }

        public <T> T[] genericArrayReturn(T[] val) {
            return val;
        }

        // A method mimicking a state machine returning a wrong type, bypassing the compiler via erasure
        @SuppressWarnings("unchecked")
        public <T> T maliciousReturn(Object val) {
            return (T) val;
        }
    }

    public void testSubmitValidInput() throws Throwable {
        Method method = MetadataTarget.class.getMethod("singleArg", String.class);
        CommandMetadata metadata = new CommandMetadata(1, 1, method,
                new CommandSchema(String.class), new CommandSchema(String.class));

        MetadataTarget target = new MetadataTarget();
        assertThat(metadata.<String>submit(target, new Object[] { "test" })).isEqualTo("test");
    }

    public void testSubmitPrimitiveWithAutoboxing() throws Throwable {
        Method method = MetadataTarget.class.getMethod("primitiveArg", int.class);
        CommandMetadata metadata = new CommandMetadata(2, 1, method,
                new CommandSchema(int.class), new CommandSchema(int.class));

        MetadataTarget target = new MetadataTarget();
        assertThat(metadata.<Object>submit(target, new Object[] { 1 })).isEqualTo(2);
    }

    public void testSubmitNoArgMethod() throws Throwable {
        Method method = MetadataTarget.class.getMethod("noArg");
        CommandMetadata metadata = new CommandMetadata(3, 1, method,
                new CommandSchema(null), new CommandSchema(void.class));

        MetadataTarget target = new MetadataTarget();
        assertThat(metadata.<Object>submit(target, new Object[] {})).isNull();
    }

    public void testSubmitGenericBounds() throws Throwable {
        Method method = MetadataTarget.class.getMethod("genericReturn", Number.class);
        Type returnType = method.getGenericReturnType();
        Type inputType = method.getGenericParameterTypes()[0];

        CommandMetadata metadata = new CommandMetadata(4, 1, method,
                new CommandSchema(inputType), new CommandSchema(returnType));

        MetadataTarget target = new MetadataTarget();
        // Integer extends Number, so this properly respects the upper bound
        assertThat(metadata.<Object>submit(target, new Object[] { 10 })).isEqualTo(10);
    }

    public void testSubmitGenericArrayBounds() throws Throwable {
        Method method = MetadataTarget.class.getMethod("genericArrayReturn", Object[].class);
        Type returnType = method.getGenericReturnType();
        Type inputType = method.getGenericParameterTypes()[0];

        CommandMetadata metadata = new CommandMetadata(5, 1, method,
                new CommandSchema(inputType), new CommandSchema(returnType));

        MetadataTarget target = new MetadataTarget();
        Integer[] array = new Integer[] { 1, 2, 3 };

        // This explicitly exercises the GenericArrayType validation in CommandSchema
        assertThat(metadata.<Object>submit(target, new Object[] { array })).isEqualTo(array);
    }

    public void testSubmitInvalidInput() throws Throwable {
        Method method = MetadataTarget.class.getMethod("singleArg", String.class);
        CommandMetadata metadata = new CommandMetadata(6, 1, method,
                new CommandSchema(String.class), new CommandSchema(String.class));

        MetadataTarget target = new MetadataTarget();

        // Expected to throw an exception because Integer is not String.
        // The input payload violates the CommandSchema.
        assertThatThrownBy(() -> metadata.submit(target, new Object[] { 123 }))
                .isInstanceOf(Exception.class);
    }

    public void testSubmitInvalidOutput() throws Throwable {
        Method method = MetadataTarget.class.getMethod("maliciousReturn", Object.class);

        // Emulate a schema that strictly expects a String return
        CommandSchema inSchema = new CommandSchema(Object.class);
        CommandSchema outSchema = new CommandSchema(String.class);

        CommandMetadata metadata = new CommandMetadata(7, 1, method, inSchema, outSchema);
        MetadataTarget target = new MetadataTarget();

        // Method returns an Integer, but schema strictly expects String.
        // Output schema validation must fail and reject the State Machine execution.
        assertThatThrownBy(() -> metadata.submit(target, new Object[] { 123 }))
                .isInstanceOf(Exception.class);
    }

    public void testCommandValidationSuccess() throws Throwable {
        Method method = MetadataTarget.class.getMethod("noArg");
        CommandMetadata metadata = new CommandMetadata(8, 2, method,
                new CommandSchema(null), new CommandSchema(void.class));

        JRaftCommand readCommand = JRaftReadCommand.create(8, 2);
        JRaftCommand writeCommand = JRaftWriteCommand.create(8, 2);

        // Neither should throw an exception since id and version perfectly match
        metadata.validate(method, readCommand);
        metadata.validate(null, writeCommand);
    }

    public void testCommandValidationIdMismatch() throws Throwable {
        Method method = MetadataTarget.class.getMethod("noArg");
        CommandMetadata metadata = new CommandMetadata(9, 1, method,
                new CommandSchema(null), new CommandSchema(void.class));

        JRaftCommand mismatchedIdCommand = JRaftReadCommand.create(10, 1);

        assertThatThrownBy(() -> metadata.validate(method, mismatchedIdCommand))
                .isInstanceOf(Exception.class);
    }

    public void testCommandValidationVersionMismatch() throws Throwable {
        Method method = MetadataTarget.class.getMethod("noArg");
        CommandMetadata metadata = new CommandMetadata(10, 2, method,
                new CommandSchema(null), new CommandSchema(void.class));

        JRaftCommand mismatchedVersionCommand = JRaftReadCommand.create(10, 1);

        assertThatThrownBy(() -> metadata.validate(method, mismatchedVersionCommand))
                .isInstanceOf(Exception.class);
    }
}
