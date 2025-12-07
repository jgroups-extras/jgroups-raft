package org.jgroups.raft.internal.metrics;

import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.metrics.ElectionMetrics;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

record ElectionMetricsCollector(BaseElection election) implements ElectionMetrics {

    @Override
    public String getLeaderRaftId() {
        return election.raft().leaderRaftId();
    }

    @Override
    public Instant getLeaderElectionTime() {
        return election.electionStart();
    }

    @Override
    public Duration getTimeSinceLastLeaderChange() {
        return Duration.of(election.timeSinceLastElection(), ChronoUnit.MILLIS);
    }
}
