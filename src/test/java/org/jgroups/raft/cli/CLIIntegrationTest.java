package org.jgroups.raft.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftMetrics;
import org.jgroups.raft.JGroupsRaftState;
import org.jgroups.raft.api.JRaftTestCluster;
import org.jgroups.raft.api.SimpleKVStateMachine;
import org.jgroups.raft.configuration.RuntimeProperties;
import org.jgroups.raft.util.internal.Json;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CLIIntegrationTest {

    private static final int CLUSTER_SIZE = 3;

    private JRaftTestCluster<SimpleKVStateMachine> cluster;

    @BeforeClass
    public void setup() throws Exception {
        cluster = JRaftTestCluster.create(SimpleKVStateMachine.Impl::new, SimpleKVStateMachine.class, CLUSTER_SIZE,
                builder -> builder.withRuntimeProperties(RuntimeProperties.from(
                        Map.of(JGroupsRaftMetrics.METRICS_ENABLED.name(), "true")
                )));
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            cluster.channel(i).getProtocolStack().getTransport().enableDiagnostics();
        }
        cluster.waitUntilLeaderElected();

        // Submit a few writes so the log is not empty.
        JGroupsRaft<SimpleKVStateMachine> leader = cluster.leader();
        leader.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("key1", "value1"));
        leader.write((Consumer<SimpleKVStateMachine>) kv -> kv.handlePut("key2", "value2"));

        // Wait for replication to complete on all nodes.
        assertThat(eventually(() -> {
            for (int i = 0; i < CLUSTER_SIZE; i++) {
                if (cluster.raft(i).state().commitIndex() < 2) return false;
            }
            return true;
        }, 10, TimeUnit.SECONDS)).isTrue();
    }

    @AfterClass
    public void teardown() throws Exception {
        if (cluster != null)
            cluster.close();
    }

    public void testStatusJsonContainsAllNodes() {
        String json = executeCLI("status", "--format", "JSON");

        assertThat(json).startsWith("[").endsWith("]");
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            String raftId = cluster.raft(i).state().id();
            assertThat(json).contains(raftId);
        }
    }

    public void testStatusResponsesMatchClusterState() {
        String json = executeCLI("status", "--format", "JSON");
        List<Map<String, Object>> nodes = parseJsonArray(json);

        assertThat(nodes).hasSizeGreaterThanOrEqualTo(CLUSTER_SIZE);

        // Exactly one leader.
        long leaderCount = nodes.stream()
                .map(n -> (Map<String, Object>) n.get("response"))
                .filter(r -> "Leader".equals(r.get("role")))
                .count();
        assertThat(leaderCount)
                .withFailMessage("Response was: " + json)
                .isOne();

        // All nodes agree on who the leader is.
        JGroupsRaftState leaderState = cluster.leader().state();
        for (Map<String, Object> node : nodes) {
            Map<String, Object> response = (Map<String, Object>) node.get("response");
            assertThat(response.get("leader")).isEqualTo(leaderState.id());
        }

        // All nodes have the same term.
        long expectedTerm = leaderState.term();
        for (Map<String, Object> node : nodes) {
            Map<String, Object> response = (Map<String, Object>) node.get("response");
            assertThat(((Number) response.get("term")).longValue()).isEqualTo(expectedTerm);
        }

        // Commit index reflects our two writes.
        for (Map<String, Object> node : nodes) {
            Map<String, Object> response = (Map<String, Object>) node.get("response");
            assertThat(((Number) response.get("commit-index")).longValue()).isGreaterThanOrEqualTo(2);
        }
    }

    public void testMetricsResponseContainsCategories() {
        String json = executeCLI("metrics", "--format", "JSON");
        List<Map<String, Object>> nodes = parseJsonArray(json);

        assertThat(nodes).hasSizeGreaterThanOrEqualTo(CLUSTER_SIZE);

        for (Map<String, Object> node : nodes) {
            Map<String, Object> data = (Map<String, Object>) node.get("response");
            assertThat(data).containsKeys("total-nodes", "active-nodes",
                    "election-metrics", "total-latency", "processing-latency",
                    "election-latency", "redirect-latency", "log-metrics");
            assertThat(((Number) data.get("total-nodes")).intValue())
                    .withFailMessage("Response was: " + json)
                    .isEqualTo(CLUSTER_SIZE);
            assertThat(((Number) data.get("active-nodes")).intValue())
                    .withFailMessage("Response was: " + json)
                    .isEqualTo(CLUSTER_SIZE);
        }
    }

    public void testLogMetricsValues() {
        String json = executeCLI("metrics", "--format", "JSON");
        List<Map<String, Object>> nodes = parseJsonArray(json);

        for (Map<String, Object> node : nodes) {
            Map<String, Object> data = (Map<String, Object>) node.get("response");
            Map<String, Object> log = (Map<String, Object>) data.get("log-metrics");

            assertThat(((Number) log.get("total-entries")).longValue())
                    .withFailMessage("Response was: " + json)
                    .isGreaterThanOrEqualTo(2);
            assertThat(((Number) log.get("committed-entries")).longValue())
                    .isGreaterThanOrEqualTo(2);
            assertThat(((Number) log.get("uncommitted-entries")).longValue())
                    .isZero();
            assertThat(((Number) log.get("current-term")).longValue())
                    .isGreaterThan(0);
            assertThat(((Number) log.get("log-size-bytes")).longValue())
                    .isGreaterThan(0);
        }
    }

    public void testStatusTableFormatRenders() {
        String table = executeCLI("status", "--format", "TABLE");

        assertThat(table).contains("─");
        assertThat(table).contains("raft-id");
        assertThat(table).contains("role");
        assertThat(table).contains("leader");
    }

    public void testStatusTextFormatRenders() {
        String text = executeCLI("status", "--format", "TEXT");

        assertThat(text).contains("role=");
        assertThat(text).contains("leader=");
        assertThat(text).contains("term=");
    }

    private String executeCLI(String... args) {
        StringWriter output = new StringWriter();
        String clusterName = cluster.channel(0).getClusterName();
        String[] fullArgs = Stream.concat(
                Stream.of(args),
                Stream.of("-cluster", clusterName, "-timeout", "500")
        ).toArray(String[]::new);

        int exitCode = JGroupsRaftCLI.internal(fullArgs, cli -> cli.setOut(new PrintWriter(output)));
        assertThat(exitCode).as("CLI command should exit successfully").isZero();

        return output.toString().trim();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> parseJsonArray(String json) {
        Map<String, Object> wrapped = Json.fromJson("{\"items\":" + json + "}");
        return (List<Map<String, Object>>) wrapped.get("items");
    }
}
