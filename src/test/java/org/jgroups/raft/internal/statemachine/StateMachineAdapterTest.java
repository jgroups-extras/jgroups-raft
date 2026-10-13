package org.jgroups.raft.internal.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.internal.registry.CommandRegistry;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.internal.serialization.binary.SerializationRegistry;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class StateMachineAdapterTest {

    public void testSnapshotReadFailureIsPropagated() {
        CounterImpl sm = new CounterImpl();
        sm.value = 7;
        StateMachine adapter = createAdapter(sm);

        InputStream failing = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("simulated read failure");
            }
        };

        assertThatThrownBy(() -> adapter.readContentFrom(new DataInputStream(failing)))
                .isInstanceOf(UncheckedIOException.class)
                .hasRootCauseMessage("simulated read failure");

        // The failed restore must not touch the current state.
        assertThat(sm.value).isEqualTo(7);
    }

    private static StateMachine createAdapter(CounterImpl sm) {
        Serializer serializer = Serializer.create(SerializationRegistry.create());
        CommandRegistry<Counter> registry = new CommandRegistry<>(sm, Counter.class);
        return new StateMachineAdapter<>(serializer, registry, sm);
    }

    @JGroupsRaftStateMachine
    private interface Counter {

        @StateMachineWrite(id = 1)
        Void set(int value);

        @StateMachineRead(id = 2)
        int get();
    }

    private static final class CounterImpl implements Counter {

        @StateMachineField(order = 0)
        private int value;

        @Override
        public Void set(int value) {
            this.value = value;
            return null;
        }

        @Override
        public int get() {
            return value;
        }
    }
}
