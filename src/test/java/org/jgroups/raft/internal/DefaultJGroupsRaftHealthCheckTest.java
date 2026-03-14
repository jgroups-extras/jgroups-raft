package org.jgroups.raft.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaftHealthCheck.ClusterHealth;
import org.jgroups.raft.tests.harness.ControlledTimeService;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class DefaultJGroupsRaftHealthCheckTest {

    private JChannel channel;
    private DefaultJGroupsRaftHealthCheck healthCheck;

    @BeforeMethod
    public void setup() throws Exception {
        channel = new JChannel();
        healthCheck = new DefaultJGroupsRaftHealthCheck(channel);
    }

    @AfterMethod
    public void teardown() {
        channel.close();
    }

    public void testNotLiveWhenDisconnected() {
        assertThat(healthCheck.isNodeLive()).isFalse();
    }

    public void testNotLiveWhenClosed() {
        channel.close();

        assertThat(healthCheck.isNodeLive()).isFalse();
    }

    public void testNotReadyWhenDisconnected() {
        assertThat(healthCheck.isNodeReady()).isFalse();
    }

    public void testNotReadyBeforeStart() {
        assertThat(healthCheck.isNodeReady()).isFalse();
    }

    public void testClusterHealthNotRunningWhenDisconnected() {
        assertThat(healthCheck.getClusterHealth()).isEqualTo(ClusterHealth.NOT_RUNNING);
    }

    public void testClusterHealthNotRunningBeforeStart() {
        assertThat(healthCheck.getClusterHealth()).isEqualTo(ClusterHealth.NOT_RUNNING);
    }

    public void testClusterHealthNotRunningAfterStop() {
        RAFT raft = new RAFT();
        healthCheck.start(raft);
        healthCheck.stop();

        assertThat(healthCheck.getClusterHealth()).isEqualTo(ClusterHealth.NOT_RUNNING);
    }

    public void testStartSetsRaft() {
        RAFT raft = new RAFT();
        healthCheck.start(raft);

        // raft is set but channel still disconnected.
        assertThat(healthCheck.getClusterHealth()).isEqualTo(ClusterHealth.NOT_RUNNING);
    }

    public void testStopClearsRaft() {
        RAFT raft = new RAFT();
        healthCheck.start(raft);
        healthCheck.stop();

        assertThat(healthCheck.isNodeReady()).isFalse();
        assertThat(healthCheck.getClusterHealth()).isEqualTo(ClusterHealth.NOT_RUNNING);
    }
}
