package org.jgroups.raft.internal.metrics;

import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.JGroupsRaftMetrics;
import org.jgroups.raft.metrics.ElectionMetrics;
import org.jgroups.raft.metrics.LogMetrics;
import org.jgroups.raft.metrics.PerformanceMetrics;

public class JGroupsRaftMetricsCollector implements JGroupsRaftMetrics {

    private final RAFT raft;
    private final ElectionMetrics electionMetrics;
    private final PerformanceMetrics performanceMetrics;

    public JGroupsRaftMetricsCollector(RAFT raft, BaseElection election) {
        this.raft = raft;
        this.electionMetrics = new ElectionMetricsCollector(election);
        SystemMetricsTracker smt = new SystemMetricsTracker();
        raft.systemMetricsTracker(smt);
        this.performanceMetrics = smt;
    }

    @Override
    public int getTotalNodes() {
        return raft.members().size();
    }

    @Override
    public int getActiveNodes() {
        return raft.getTransport().view().size();
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
        return performanceMetrics;
    }
}
