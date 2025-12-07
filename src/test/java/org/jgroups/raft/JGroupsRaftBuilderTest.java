package org.jgroups.raft;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.internal.DangerousJGroupsRaftUtil;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.api.TestSerializationInitializerImpl;

import java.util.Collections;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftBuilderTest {

    public void testBuilderWithConfigurationFile() {
        JGroupsRaft.Builder<SimpleKVStateMachine> builder = JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                .withJGroupsConfig("test-raft.xml")
                .withClusterName("lifecycle-test")
                .registerSerializationContextInitializer(new TestSerializationInitializerImpl())
                .configureRaft()
                    .withRaftId("single_node")
                    .withMembers(Collections.singletonList("single_node"))
                    .withLogClass(InMemoryLog.class)
                    .withUseFsync(true)
                    .withLogDirectory("some-directory")
                    .withLogPrefix("some-prefix")
                    .withMaxLogSize(4096)
                    .withResendInterval(2000)
                    .withSendCommitsImmediately(true)
                .and();
        JGroupsRaft<SimpleKVStateMachine> raft = builder.build();
        raft.start();

        JChannel channel = DangerousJGroupsRaftUtil.extractJChannel(raft);
        RAFT r = RAFT.findProtocol(RAFT.class, channel.getProtocolStack().getTopProtocol(), true);
        assertThat(r).isNotNull();

        assertThat(r.raftId()).isEqualTo("single_node");
        assertThat(r.members()).containsExactly("single_node");
        assertThat(r.logClass()).isEqualTo(InMemoryLog.class.getName());
        assertThat(r.logUseFsync()).isTrue();
        assertThat(r.logDir()).isEqualTo("some-directory");
        assertThat(r.logPrefix()).isEqualTo("some-prefix");
        assertThat(r.maxLogSize()).isEqualTo(4096);
        assertThat(r.resendInterval()).isEqualTo(2000);
        assertThat(r.sendCommitsImmediately()).isTrue();

        raft.stop();
    }

    public void testBuilderWithJChannel() throws Exception {
        JChannel channel = new JChannel("test-raft.xml");
        channel.setName("lifecycle-test-2");
        JGroupsRaft.Builder<SimpleKVStateMachine> builder = JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                .withJChannel(channel)
                .withClusterName("lifecycle-test-2")
                .registerSerializationContextInitializer(new TestSerializationInitializerImpl())
                .configureRaft()
                    .withRaftId("single_node")
                    .withMembers(Collections.singletonList("single_node"))
                    .withLogClass(InMemoryLog.class)
                    .withUseFsync(true)
                    .withLogDirectory("some-directory")
                    .withLogPrefix("some-prefix")
                    .withMaxLogSize(4096)
                    .withResendInterval(2000)
                    .withSendCommitsImmediately(true)
                .and();
        JGroupsRaft<SimpleKVStateMachine> raft = builder.build();
        raft.start();

        RAFT r = RAFT.findProtocol(RAFT.class, channel.getProtocolStack().getTopProtocol(), true);
        assertThat(r).isNotNull();

        assertThat(r.raftId()).isEqualTo("single_node");
        assertThat(r.members()).containsExactly("single_node");
        assertThat(r.logClass()).isEqualTo(InMemoryLog.class.getName());
        assertThat(r.logUseFsync()).isTrue();
        assertThat(r.logDir()).isEqualTo("some-directory");
        assertThat(r.logPrefix()).isEqualTo("some-prefix");
        assertThat(r.maxLogSize()).isEqualTo(4096);
        assertThat(r.resendInterval()).isEqualTo(2000);
        assertThat(r.sendCommitsImmediately()).isTrue();

        raft.stop();
    }

    public void testInvalidConfiguration() {
        // Creating a builder with file and without cluster name is not allowed
        assertThatThrownBy(() ->
                JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                        .withJGroupsConfig("test-raft.xml")
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("clusterName");

        // Creating a builder with a disconnected JChannel and without cluster name is not allowed
        assertThatThrownBy(() ->
                JGroupsRaft.builder(new SimpleKVStateMachine.Impl(), SimpleKVStateMachine.class)
                        .withJChannel(new JChannel())
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("clusterName");
    }
}
