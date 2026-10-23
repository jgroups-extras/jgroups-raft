package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;
import org.jgroups.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftAPITest {

    @Test(dataProvider = "arguments")
    public void testClusteringAndOperations(int size, boolean fromLeader) throws Exception {
        testSetupAndDestroy(size, fromLeader);
    }

    @Test(dataProvider = "arguments")
    public void testClusteringAndOperationsWithOptions(int size, boolean fromLeader) throws Exception {
        testSetupAndDestroyWithOptions(size, fromLeader);
    }

    public void testInstanceLifecycle() {
        JGroupsRaft.Builder<SimpleKVStateMachine> builder = JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                .withJGroupsConfig("test-raft.xml")
                .withClusterName("lifecycle-test");
        builder.configureRaft()
                .withRaftId("single_node")
                .withMembers(Collections.singletonList("single_node"));
        JGroupsRaft<SimpleKVStateMachine> raft = builder.build();

        assertThat(raft.role()).isEqualTo(JGroupsRaftRole.NONE);

        // Assert an operation is not accepted before start.
        assertThatThrownBy(() -> raft.read(kv -> kv.handleGet("key")))
                .isInstanceOf(IllegalStateException.class);

        raft.start();

        assertThat(eventually(() -> raft.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS))
                .isTrue();

        // Assert an operation is accepted after start.
        assertThat(raft.read((Function<SimpleKVStateMachine, String>)  kv -> kv.handleGet("key")))
                .isNull();
        assertThat(raft.role()).isEqualTo(JGroupsRaftRole.LEADER);

        raft.stop();

        // Assert an operation is not accepted after stop.
        assertThatThrownBy(() -> raft.read(kv -> kv.handleGet("key")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(raft.role()).isEqualTo(JGroupsRaftRole.NONE);

        // Assert the instance can not be reused after stop.
        assertThatThrownBy(raft::start)
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * An instance built over a channel the application already connected cannot be restarted either.
     * <p>
     * Stopping leaves an application-owned channel open, so the instance must refuse a restart on its own rather than
     * rely on the channel being closed.
     * </p>
     */
    public void testInstanceOnConnectedChannelCannotRestart() throws Exception {
        String cluster = "lifecycle-connected-" + System.nanoTime();
        JChannel channel = connectedRaftChannel("single_node", List.of("single_node"), cluster);

        try {
            JGroupsRaft<SimpleKVStateMachine> jgr = buildOnConnectedChannel(channel, cluster);
            jgr.start();
            assertThat(eventually(() -> jgr.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS))
                    .isTrue();

            jgr.stop();
            assertThat(channel.isConnected())
                    .as("Stop leaves an application-owned channel connected")
                    .isTrue();

            assertThatThrownBy(jgr::start)
                    .isInstanceOf(IllegalStateException.class);

            // The rejected restart must not touch the instance; the channel keeps running, so a partial start would
            // pick up its current role.
            assertThat(jgr.role())
                    .as("A rejected restart leaves the instance stopped")
                    .isEqualTo(JGroupsRaftRole.NONE);
        } finally {
            Util.close(channel);
        }
    }

    /**
     * A stopped instance stays stopped while its application-owned channel keeps running.
     * <p>
     * Raft keeps working on the channel after the instance stops, so role changes still happen underneath. The stopped
     * instance must not report them.
     * </p>
     */
    public void testStoppedInstanceIgnoresRoleChangesOnConnectedChannel() throws Exception {
        String cluster = "lifecycle-role-" + System.nanoTime();
        List<String> members = List.of("A", "B");
        JChannel a = connectedRaftChannel("A", members, cluster);
        JChannel b = connectedRaftChannel("B", members, cluster);

        try {
            JGroupsRaft<SimpleKVStateMachine> raftA = buildOnConnectedChannel(a, cluster);
            JGroupsRaft<SimpleKVStateMachine> raftB = buildOnConnectedChannel(b, cluster);
            raftA.start();
            raftB.start();

            // A is the coordinator, and with equal logs the coordinator elects itself.
            assertThat(eventually(() -> raftA.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS))
                    .isTrue();

            raftA.stop();
            assertThat(raftA.role()).isEqualTo(JGroupsRaftRole.NONE);

            // B leaving takes away A's majority, so the RAFT protocol on A's channel steps down from leader.
            RAFT protocolA = RAFT.findProtocol(RAFT.class, a.getProtocolStack().getTopProtocol(), true);
            raftB.stop();
            Util.close(b);
            assertThat(eventually(() -> !protocolA.isLeader(), 10, TimeUnit.SECONDS))
                    .as("A should step down after losing the majority")
                    .isTrue();

            assertThat(raftA.role())
                    .as("A stopped instance does not follow role changes on the channel")
                    .isEqualTo(JGroupsRaftRole.NONE);
        } finally {
            Util.close(b, a);
        }
    }

    public void testSuccessiveLifecycleCalls() {
        JGroupsRaft.Builder<SimpleKVStateMachine> builder = JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                .withJGroupsConfig("test-raft.xml")
                .withClusterName("lifecycle-successive-test");
        builder.configureRaft()
                .withRaftId("single_node")
                .withMembers(Collections.singletonList("single_node"));
        JGroupsRaft<SimpleKVStateMachine> raft = builder
                .build();

        assertThat(raft.role()).isEqualTo(JGroupsRaftRole.NONE);

        raft.start();
        assertThat(eventually(() -> raft.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS))
                .isTrue();

        // Invoke start again without problems, this results in a no-op.
        raft.start();

        // Stop the raft instance multiple times.
        raft.stop();
        assertThat(raft.role()).isEqualTo(JGroupsRaftRole.NONE);
        raft.stop();
    }

    /**
     * Creates and connects a channel with the Raft stack, so the application owns it rather than {@link JGroupsRaft}.
     */
    private static JChannel connectedRaftChannel(String raftId, List<String> members, String cluster) throws Exception {
        RAFT raft = new RAFT()
                .raftId(raftId)
                .members(members)
                .logClass(InMemoryLog.class.getCanonicalName())
                .logPrefix(cluster + "-" + raftId);
        JChannel channel = new JChannel(
                new SHARED_LOOPBACK(),
                new SHARED_LOOPBACK_PING(),
                new NAKACK2(),
                new UNICAST3(),
                new STABLE(),
                new GMS().setJoinTimeout(1000),
                new ELECTION(),
                raft,
                new REDIRECT()
        ).name(raftId);
        channel.connect(cluster);
        return channel;
    }

    private static JGroupsRaft<SimpleKVStateMachine> buildOnConnectedChannel(JChannel channel, String cluster) {
        return JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                .withJChannel(channel)
                .withClusterName(cluster)
                .build();
    }

    @DataProvider
    Object[][] arguments() {
        return new Object[][] {
                { 3, true },
                { 3, false },
                { 5, true },
                { 5, false },
        };
    }

    private void testSetupAndDestroy(int size, boolean fromLeader) throws Exception {
        JRaftTestCluster<SimpleKVStateMachine> cluster = JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, size);

        cluster.waitUntilLeaderElected();

        JGroupsRaft<SimpleKVStateMachine> raft = fromLeader
                ? cluster.leader()
                : cluster.follower();

        assertThat(raft.read((Function<SimpleKVStateMachine, String>) kv -> kv.handleGet("hello")))
                .isNull();

        raft.write(kv -> kv.handlePut("hello", "world"));

        assertThat(raft.<String>read(kv -> kv.handleGet("hello")))
                .isEqualTo("world");

        cluster.close();
    }

    private void testSetupAndDestroyWithOptions(int size, boolean fromLeader) throws Exception {
        JRaftTestCluster<SimpleKVStateMachine> cluster = JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, size);

        cluster.waitUntilLeaderElected();

        JGroupsRaft<SimpleKVStateMachine> raft = fromLeader
                ? cluster.leader()
                : cluster.follower();

        assertThat(raft.<String>read(kv -> kv.handleGet("hello"), JGroupsRaftCommandOptions.readOptions().build()))
                .isNull();

        raft.write(kv -> kv.handlePut("hello", "world"), JGroupsRaftCommandOptions.writeOptions().build());

        assertThat(raft.<String>read(kv -> kv.handleGet("hello"), JGroupsRaftCommandOptions.readOptions().build()))
                .isEqualTo("world");

        cluster.close();
    }
}
