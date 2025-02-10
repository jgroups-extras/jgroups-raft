package org.jgroups.raft.internal.metrics;

import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.metrics.ElectionMetrics;

import java.time.Duration;
import java.time.Instant;

public class ElectionMetricsCollector implements ElectionMetrics {

    private final BaseElection election;

    public ElectionMetricsCollector(BaseElection election) {
        this.election = election;
    }

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
        return Duration.between(election.electionStart(), election.electionEnd());
    }
}
