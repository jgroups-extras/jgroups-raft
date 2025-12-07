package org.jgroups.raft.internal.metrics;

import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.metrics.PerformanceMetrics;

public final class SystemMetricsTracker implements PerformanceMetrics {

    private final LatencyTracker commandProcessingTracker = new LatencyTracker();
    private final LatencyTracker replicationTracker = new LatencyTracker();
    private final LatencyTracker electionTracker = new LatencyTracker();

    public void recordCommandProcessingLatency(long latencyNanos) {
        commandProcessingTracker.recordLatency(latencyNanos);
    }

    public void recordReplicationLatency(long latencyNanos) {
        replicationTracker.recordLatency(latencyNanos);
    }

    public void recordElectionLatency(long latencyNanos) {
        electionTracker.recordLatency(latencyNanos);
    }

    @Override
    public LatencyMetrics getCommandProcessingLatency() {
        return commandProcessingTracker;
    }

    @Override
    public LatencyMetrics getLeaderElectionLatency() {
        return electionTracker;
    }

    @Override
    public LatencyMetrics getReplicationLatency() {
        return replicationTracker;
    }
}
