package org.jgroups.raft;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.jgroups.tests.DummyStateMachine;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ReplicatedStateMachineTest {

    protected static final String CLUSTER = ReplicatedStateMachineTest.class.getSimpleName();
    protected final List<String> mbrs = Arrays.asList("A", "B", "C", "D");

    public void testEquals() throws Exception {
        try (JChannel channelA = create("A");
             JChannel channelB = create("B")) {
            ReplicatedStateMachine<String, String> one = new ReplicatedStateMachine<>(channelA);
            ReplicatedStateMachine<String, String> other = new ReplicatedStateMachine<>(channelB);

            assert one.equals(one);
            assert one.equals(other);
            assert other.equals(one);
            assert !one.equals(null);
            assert !one.equals(new Object());
        }
    }

    @SuppressWarnings("resource")
    protected JChannel create(String name) throws Exception {
        RAFT raft = new RAFT().members(mbrs).raftId(name).stateMachine(new DummyStateMachine())
                .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(name + "-" + CLUSTER);
        JChannel ch = new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(name);
        ch.connect(CLUSTER);
        return ch;
    }
}
