package org.jgroups.raft.internal.statemachine;

import org.jgroups.Global;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.internal.registry.SerializationRegistry;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.tests.serialization.TestSerializationInitializerImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Objects;

import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class StateMachineSnapshotterTest {

    public void testWriteAndReadSnapshot() {
        final class Impl implements SnapshotStateMachine {
            @StateMachineField(order = 0)
            private int counter = 42;

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof Impl other))
                    return false;
                return other.counter == counter;
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(counter);
            }
        }
        Impl sm = new Impl();
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        Serializer serializer = Serializer.protoStream(registry);

        StateMachineSnapshotter<SnapshotStateMachine> snapshotter = new StateMachineSnapshotter<>(sm, serializer);

        byte[] datum = snapshotter.writeSnapshot();

        sm.counter = 0; // Reset the state machine to ensure the snapshot is read correctly

        snapshotter.readSnapshot(datum);
        assertThat(sm.counter).isEqualTo(42);
    }

    public void testWriteAndReadSnapshotWithMultipleFields() {
        final class Impl implements SnapshotStateMachine {
            @StateMachineField(order = 0)
            private int counter = 42;

            @StateMachineField(order = 1)
            private String name = "test";

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof Impl other))
                    return false;
                return other.counter == counter && Objects.equals(other.name, name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(counter, name);
            }
        }
        Impl sm = new Impl();
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        Serializer serializer = Serializer.protoStream(registry);

        StateMachineSnapshotter<SnapshotStateMachine> snapshotter = new StateMachineSnapshotter<>(sm, serializer);

        byte[] datum = snapshotter.writeSnapshot();

        sm.counter = 0; // Reset the state machine to ensure the snapshot is read correctly
        sm.name = null;

        snapshotter.readSnapshot(datum);
        assertThat(sm.counter).isEqualTo(42);
        assertThat(sm.name).isEqualTo("test");
    }

    public void testFinalFieldNotAllowed() {
        final class Impl implements SnapshotStateMachine {
            @StateMachineField(order = 0)
            private final int counter = 42;
        }
        Impl sm = new Impl();
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        Serializer serializer = Serializer.protoStream(registry);

        assertThatThrownBy(() -> new StateMachineSnapshotter<>(sm, serializer))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("is final, which is not allowed to load snapshots");
    }

    public void testMultipleFieldsWithSameOrderNotAllowed() {
        final class Impl implements SnapshotStateMachine {
            @StateMachineField(order = 0)
            private int counter = 42;

            @StateMachineField(order = 0)
            private String name = "test";
        }
        Impl sm = new Impl();
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        Serializer serializer = Serializer.protoStream(registry);

        assertThatThrownBy(() -> new StateMachineSnapshotter<>(sm, serializer))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("multiple fields with order 0");
    }


    private interface SnapshotStateMachine extends StateMachine {

        @Override
        default void writeContentTo(DataOutput out) throws Exception { }

        @Override
        default void readContentFrom(DataInput in) throws Exception { }
    }
}
