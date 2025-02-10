package org.jgroups.raft.internal.statemachine;

import org.jgroups.Global;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.tests.api.JRaftTestCluster;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Field;
import java.util.function.Consumer;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftSnapshotTest {

    public void testStateMachineSnapshot() throws Throwable {
        JRaftTestCluster<StateMachineTest> cluster = JRaftTestCluster.create(StateMachineTestImpl::new, StateMachineTest.class, 3);

        cluster.waitUntilLeaderElected();

        JGroupsRaft<StateMachineTest> leader = cluster.leader();
        for (int i = 0; i < 10; i++) {
            leader.write((Consumer<StateMachineTest>) smt -> smt.add(1));
        }

        int index = cluster.leaderIndex();
        RAFT raft = cluster.raftProtocol(index);
        raft.snapshot();

        assertThat(raft.numSnapshots()).isOne();

        StateMachineTestImpl sm = (StateMachineTestImpl) cluster.stateMachine(index);
        sm.value = 0;

        assertThat(sm.get()).isZero();
        assertThat(leader.read(StateMachineTest::get)).isZero();

        hackRaftRestored(raft);

        raft.initStateMachineFromLog();
        assertThat(leader.read(StateMachineTest::get)).isEqualTo(10);
        assertThat(sm.get()).isEqualTo(10);
    }

    private static void hackRaftRestored(RAFT raft) {
        // Force the RAFT instance to think it is not loaded yet.
        Field stateMachineLoadedField = Util.getField(RAFT.class, "state_machine_loaded");
        Util.setField(stateMachineLoadedField, raft, false);
    }

    @JGroupsRaftStateMachine
    private interface StateMachineTest extends StateMachine {

        @StateMachineWrite(id = 1)
        void add(int value);

        @StateMachineRead(id = 2)
        int get();

        @Override
        default void readContentFrom(DataInput in) throws Exception { }

        @Override
        default void writeContentTo(DataOutput out) throws Exception { }
    }

    private final class StateMachineTestImpl implements StateMachineTest {

        @StateMachineField(order = 0)
        private int value;

        @Override
        public void add(int value) {
            this.value += value;
        }

        @Override
        public int get() {
            return value;
        }
    }
}
