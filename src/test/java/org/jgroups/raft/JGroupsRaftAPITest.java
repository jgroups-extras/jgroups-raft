package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.command.JGroupsRaftCommandOptions;

import java.util.Collections;
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
