package org.jgroups.raft.internal.probe;

import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaftMetrics;
import org.jgroups.raft.JGroupsRaftState;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.util.internal.Json;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.util.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RaftProtocolProbe implements DiagnosticsHandler.ProbeHandler {

    public static final String PROBE_RESPONSE_KEY = "response";
    public static final String PROBE_RAFT_STATUS = "raft-status";
    public static final String PROBE_RAFT_METRICS = "raft-metrics";

    private final RAFT raft;
    private final JGroupsRaftState state;
    private final JGroupsRaftMetrics metrics;

    public RaftProtocolProbe(RAFT raft, JGroupsRaftState state, JGroupsRaftMetrics metrics) {
        this.raft = raft;
        this.state = state;
        this.metrics = metrics;
    }

    @Override
    public Map<String, String> handleProbe(String... keys) {
        for (String key : keys) {
            switch (key) {
                case PROBE_RAFT_STATUS -> {
                    return processResponseObject(handleStatusRequest());
                }

                case PROBE_RAFT_METRICS -> {
                    return processResponseObject(handleMetricsRequest());
                }
            }
        }
        return null;
    }

    private Map<String, String> processResponseObject(Map<String, ?> response) {
        return Map.of(PROBE_RESPONSE_KEY, Json.toJson(response));
    }

    private Map<String, Object> handleStatusRequest() {
        Map<String, Object> response = new HashMap<>();
        response.put("node", raft.raftId());
        response.put("role", raft.role());
        response.put("leader", raft.leaderRaftId());
        response.put("term", state.term());
        response.put("last-applied", state.lastApplied());
        response.put("commit-index", state.commitIndex());
        response.put("members", raft.members());
        return response;
    }

    private Map<String, Object> handleMetricsRequest() {
        Map<String, Object> response = new HashMap<>();
        response.put("total-nodes", metrics.getTotalNodes());
        response.put("active-nodes", metrics.getActiveNodes());

        Map<String, String> electionMetrics = new HashMap<>();
        electionMetrics.put("leader-raft-id", metrics.leaderMetrics().getLeaderRaftId());
        electionMetrics.put("leader-election-time", String.valueOf(metrics.leaderMetrics().getLeaderElectionTime()));
        electionMetrics.put("leader-time-last-change", String.valueOf(metrics.leaderMetrics().getTimeSinceLastLeaderChange()));

        Map<String, Object> totalMetrics = new HashMap<>();
        populateLatencyMetrics(totalMetrics, metrics.performanceMetrics().getTotalLatency());

        Map<String, Object> processingMetrics = new HashMap<>();
        populateLatencyMetrics(processingMetrics, metrics.performanceMetrics().getProcessingLatency());

        Map<String, Object> electionLatencyMetrics = new HashMap<>();
        populateLatencyMetrics(electionLatencyMetrics, metrics.performanceMetrics().getLeaderElectionLatency());

        Map<String, Object> redirectMetrics = new HashMap<>();
        populateLatencyMetrics(redirectMetrics, metrics.performanceMetrics().getRedirectLatency());

        response.put("election-metrics", electionMetrics);
        response.put("total-latency", totalMetrics);
        response.put("processing-latency", processingMetrics);
        response.put("election-latency", electionLatencyMetrics);
        response.put("redirect-latency", redirectMetrics);
        return response;
    }

    private void populateLatencyMetrics(Map<String, Object> replicationMetrics, LatencyMetrics replicationValues) {
        replicationMetrics.put("avg-latency", Util.printTime(replicationValues.getAvgLatency(), TimeUnit.NANOSECONDS));
        replicationMetrics.put("max-latency", Util.printTime(replicationValues.getMaxLatency(), TimeUnit.NANOSECONDS));
        replicationMetrics.put("p99", Util.printTime(replicationValues.getP99Latency(), TimeUnit.NANOSECONDS));
        replicationMetrics.put("p95", Util.printTime(replicationValues.getP95Latency(), TimeUnit.NANOSECONDS));
        replicationMetrics.put("total-sample", replicationValues.getTotalMeasurements());
    }

    @Override
    public String[] supportedKeys() {
        return new String[] {
                PROBE_RAFT_STATUS,
                PROBE_RAFT_METRICS,
        };
    }
}
