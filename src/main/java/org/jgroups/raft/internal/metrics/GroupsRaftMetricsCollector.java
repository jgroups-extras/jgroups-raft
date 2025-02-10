package org.jgroups.raft.internal.metrics;

import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.JGroupsRaftMetrics;
import org.jgroups.raft.metrics.ElectionMetrics;
import org.jgroups.raft.metrics.LogMetrics;
import org.jgroups.raft.metrics.PerformanceMetrics;

public class GroupsRaftMetricsCollector implements JGroupsRaftMetrics {

    private final ElectionMetrics electionMetrics;

    public GroupsRaftMetricsCollector(RAFT raft, BaseElection election) {
        this.electionMetrics = new ElectionMetricsCollector(election);
    }

    @Override
    public int getTotalNodes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getActiveNodes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ElectionMetrics leaderMetrics() {
        return electionMetrics;
    }

    @Override
    public LogMetrics replicationMetrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PerformanceMetrics performanceMetrics() {
        throw new UnsupportedOperationException();
    }
}
