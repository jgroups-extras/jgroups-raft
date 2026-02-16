package org.jgroups.raft.internal.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CommandRegistryTest {

    public void testCorrectStateMachine() throws Throwable {
        TestStateMachine simple = new TestStateMachine() {

            @StateMachineRead(id = 1)
            public int add(int a) {
                return a + 1;
            }

            @StateMachineRead(id = 2)
            public int sub(int a) {
                return a - 1;
            }
        };

        CommandRegistry<TestStateMachine> registry = new CommandRegistry<>(simple, TestStateMachine.class);
        registry.initialize();

        ReplicatedMethodWrapper add = registry.getCommand(1);
        assertThat(add).isNotNull();
        assertThat(add.<Integer>submit(1)).isEqualTo(2);

        ReplicatedMethodWrapper sub = registry.getCommand(2);
        assertThat(sub).isNotNull();
        assertThat(sub.<Integer>submit(1)).isEqualTo(0);
    }

    public void testDuplicatedIds() {
        TestStateMachine simple = new TestStateMachine() {

            @StateMachineRead(id = 1)
            public int add(int a) {
                return a + 1;
            }

            @StateMachineRead(id = 1)
            public int sub(int a) {
                return a - 1;
            }
        };

        CommandRegistry<TestStateMachine> registry = new CommandRegistry<>(simple, TestStateMachine.class);
        assertThatThrownBy(registry::initialize).isInstanceOf(IllegalStateException.class);
    }

    public void testManyArguments() throws Throwable {
        TestStateMachine simple = new TestStateMachine() {
            @StateMachineRead(id = 1)
            public int add(int a, int b) {
                return a + b;
            }
        };

        CommandRegistry<TestStateMachine> registry = new CommandRegistry<>(simple, TestStateMachine.class);
        registry.initialize();

        ReplicatedMethodWrapper add = registry.getCommand(1);
        assertThat(add.<Integer>submit(2, 2)).isEqualTo(4);
    }

    public void testObjectReturnType() throws Throwable {
        TestStateMachine simple = new TestStateMachine() {
            @StateMachineRead(id = 1)
            public Object add(int a) {
                return a + 1;
            }
        };

        CommandRegistry<TestStateMachine> registry = new CommandRegistry<>(simple, TestStateMachine.class);
        registry.initialize();

        ReplicatedMethodWrapper add = registry.getCommand(1);
        assertThat(add).isNotNull();
        assertThat(add.<Object>submit(1)).isEqualTo(2);
    }

    public void testReturningClass() throws Throwable {
        TestStateMachine simple = new TestStateMachine() {
            @StateMachineRead(id = 1)
            public Map.Entry<String, String> add(int a) {
                return Map.entry(String.valueOf(a), String.valueOf(a + 1));
            }
        };

        CommandRegistry<TestStateMachine> registry = new CommandRegistry<>(simple, TestStateMachine.class);
        registry.initialize();

        ReplicatedMethodWrapper add = registry.getCommand(1);
        assertThat(add).isNotNull();
        Map.Entry<String, String> response = add.submit(1);
        assertThat(Map.ofEntries(response)).containsEntry("1", "2");
    }

    public void testGenericTypes() throws Throwable {
        @JGroupsRaftStateMachine
        interface Generic<T> {

            @StateMachineRead(id = 1)
            T get(T a, T b);

            @StateMachineRead(id = 2)
            T other(String something, T t);

            @StateMachineWrite(id = 3)
            <K> K mapTo(T t);

            @StateMachineRead(id = 4)
            <O extends CharSequence> O transformToString(T t);
        }

        Generic<Integer> g = new Generic<>() {
            @Override
            public Integer get(Integer a, Integer b) {
                return a + b;
            }

            @Override
            public Integer other(String something, Integer integer) {
                return 0;
            }

            @Override
            public <K> K mapTo(Integer integer) {
                return (K) integer;
            }

            @Override
            public <O extends CharSequence> O transformToString(Integer integer) {
                return (O) String.valueOf(integer);
            }

        };

        CommandRegistry<Generic> registry = new CommandRegistry<>(g, Generic.class);
        registry.initialize();

        assertThat(registry.getCommand(1).<Integer>submit(1, 1)).isEqualTo(2);
        assertThat(registry.getCommand(2).<Integer>submit("something", 1)).isEqualTo(0);
        assertThat(registry.getCommand(3).<Integer>submit(1)).isEqualTo(1);
        assertThat(registry.getCommand(4).<String>submit(1)).isEqualTo("1");
    }

    public void testArraysAndCollections() throws Throwable {
        @JGroupsRaftStateMachine
        interface ArraysAndCollections<E> {
            @StateMachineRead(id = 1)
            List<String> concreteType(E e);

            @StateMachineRead(id = 2)
            List<E> genericType(E e);

            @StateMachineRead(id = 3)
            String[] concreteTypeArray(E e);

            @StateMachineRead(id = 4)
            E[] genericTypeArray(E e);

            @StateMachineRead(id = 5)
            <K> List<K> genericTypeInPlace(E e);

            @StateMachineRead(id = 6)
            <K> K[] genericTypeInPlaceArray(E e);

            @StateMachineRead(id = 7)
            Set<E> genericTypeSet(E e);

            @StateMachineRead(id = 8)
            Set<String> concreteTypeSet(E e);
        }
        ArraysAndCollections<Integer> aac = new  ArraysAndCollections<>() {


            @Override
            public List<String> concreteType(Integer integer) {
                return List.of(String.valueOf(integer));
            }

            @Override
            public List<Integer> genericType(Integer integer) {
                return List.of(integer);
            }

            @Override
            public String[] concreteTypeArray(Integer integer) {
                return new String[] { String.valueOf(integer) };
            }

            @Override
            public Integer[] genericTypeArray(Integer integer) {
                return new Integer[] { integer };
            }

            @Override
            public <K> List<K> genericTypeInPlace(Integer integer) {
                return (List<K>) List.of(integer);
            }

            @Override
            public <K> K[] genericTypeInPlaceArray(Integer integer) {
                return (K[]) new Integer[] { integer };
            }

            @Override
            public Set<Integer> genericTypeSet(Integer integer) {
                return Set.of(integer);
            }

            @Override
            public Set<String> concreteTypeSet(Integer integer) {
                return Set.of(String.valueOf(integer));
            }
        };

        CommandRegistry<ArraysAndCollections> registry = new CommandRegistry<>(aac, ArraysAndCollections.class);
        registry.initialize();

        assertThat(registry.getCommand(1).<Object>submit(1)).isEqualTo(List.of("1"));
        assertThat(registry.getCommand(2).<Object>submit(1)).isEqualTo(List.of(1));
        assertThat(registry.getCommand(3).<Object>submit(1)).isEqualTo(new String[] { "1" });
        assertThat(registry.getCommand(4).<Object>submit(1)).isEqualTo(new Integer[] { 1 });
        assertThat(registry.getCommand(5).<Object>submit(1)).isEqualTo(List.of(1));
        assertThat(registry.getCommand(6).<Object>submit(1)).isEqualTo(new Integer[] { 1 });
        assertThat(registry.getCommand(7).<Object>submit(1)).isEqualTo(Set.of(1));
        assertThat(registry.getCommand(8).<Object>submit(1)).isEqualTo(Set.of("1"));
    }

    public void testBoundedArrayFailure() throws Throwable {
        @JGroupsRaftStateMachine
        interface Bounded<E extends Number> {
            @StateMachineRead(id = 1)
            E[] genericTypeArray(E e);

            @StateMachineRead(id = 2)
            List<E> genericTypeList(E e);
        }

        Bounded<Integer> b = new Bounded<>() {
            @Override
            public Integer[] genericTypeArray(Integer e) {
                return new Integer[] { e };
            }

            @Override
            public List<Integer> genericTypeList(Integer e) {
                return List.of(e);
            }
        };

        CommandRegistry<Bounded> registry = new CommandRegistry<>(b, Bounded.class);
        registry.initialize();

        assertThat(registry.getCommand(2).<Object>submit(1)).isEqualTo(List.of(1));
        assertThat(registry.getCommand(1).<Object>submit(1)).isEqualTo(new Integer[] { 1 });
    }

    @JGroupsRaftStateMachine
    public interface TestStateMachine extends StateMachine {

        @Override
        default void readContentFrom(DataInput in) throws Exception { }

        @Override
        default void writeContentTo(DataOutput out) throws Exception { }
    }
}
