package org.jgroups.raft.internal.statemachine;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.internal.registry.SerializationRegistry;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.serialization.TestSerializationInitializerImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.infinispan.protostream.types.java.CommonContainerTypesSchema;
import org.testng.annotations.Test;

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
            .hasMessageContaining("with final modifier");
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

    public void testFieldsInSuperclassAreSnapshotted() {
        abstract class BaseMachine implements SnapshotStateMachine {
            @StateMachineField(order = 0)
            protected int baseCounter = 10;
        }

        final class ChildMachine extends BaseMachine {
            @StateMachineField(order = 1)
            private String childName = "child";
        }

        ChildMachine sm = new ChildMachine();
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        Serializer serializer = Serializer.protoStream(registry);

        StateMachineSnapshotter<SnapshotStateMachine> snapshotter = new StateMachineSnapshotter<>(sm, serializer);

        byte[] datum = snapshotter.writeSnapshot();

        sm.baseCounter = 0;
        sm.childName = null;

        snapshotter.readSnapshot(datum);
        assertThat(sm.baseCounter).isEqualTo(10);
        assertThat(sm.childName).isEqualTo("child");
    }

    public void testStaticFieldNotAllowed() {
        final class Impl implements SnapshotStateMachine {
            @StateMachineField(order = 0)
            private static int counter = 42;
        }
        Impl sm = new Impl();
        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        Serializer serializer = Serializer.protoStream(registry);

        assertThatThrownBy(() -> new StateMachineSnapshotter<>(sm, serializer))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("with static modifier");
    }

    public void testSnapshotWithNullAndCollections() {
        final class Impl implements SnapshotStateMachine {
            @StateMachineField(order = 0)
            private String nullField = "not-null-yet";

            @StateMachineField(order = 1)
            private List<Integer> list = new ArrayList<>(List.of(1, 2, 3));
        }

        Impl sm = new Impl();
        // Explicitly set to null BEFORE snapshot
        // Snapshotter should handle null values correctly.
        sm.nullField = null;

        SerializationRegistry registry = SerializationRegistry.create();
        registry.register(new TestSerializationInitializerImpl());
        registry.register(new CommonContainerTypesSchema());
        Serializer serializer = Serializer.protoStream(registry);

        StateMachineSnapshotter<SnapshotStateMachine> snapshotter = new StateMachineSnapshotter<>(sm, serializer);
        byte[] datum = snapshotter.writeSnapshot();

        sm.nullField = "changed";
        sm.list = new ArrayList<>(List.of(9, 9, 9));

        snapshotter.readSnapshot(datum);
        assertThat(sm.nullField).isNull();
        assertThat(sm.list).isEqualTo(List.of(1, 2, 3));
    }

    private interface SnapshotStateMachine { }
}
