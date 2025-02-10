package org.jgroups.raft.internal.registry;

import org.jgroups.Global;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineRead;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

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

        ReplicatedMethodWrapper<Integer> add = registry.getCommand(1);
        assertThat(add).isNotNull();
        assertThat(add.submit(1)).isEqualTo(2);

        ReplicatedMethodWrapper<Integer> sub = registry.getCommand(2);
        assertThat(sub).isNotNull();
        assertThat(sub.submit(1)).isEqualTo(0);
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

        ReplicatedMethodWrapper<Integer> add = registry.getCommand(1);
        assertThat(add.submit(2, 2)).isEqualTo(4);
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

        ReplicatedMethodWrapper<Object> add = registry.getCommand(1);
        assertThat(add).isNotNull();
        assertThat(add.submit(1)).isEqualTo(2);
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

        ReplicatedMethodWrapper<Map.Entry<String, String>> add = registry.getCommand(1);
        assertThat(add).isNotNull();
        Map.Entry<String, String> response = add.submit(1);
        assertThat(Map.ofEntries(response)).containsEntry("1", "2");
    }

    @JGroupsRaftStateMachine
    public interface TestStateMachine extends StateMachine {

        @Override
        default void readContentFrom(DataInput in) throws Exception { }

        @Override
        default void writeContentTo(DataOutput out) throws Exception { }
    }
}
