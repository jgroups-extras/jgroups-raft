package org.jgroups.raft.internal.metrics;

import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.JGroupsRaftMetrics;
import org.jgroups.raft.metrics.ElectionMetrics;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.metrics.LogMetrics;
import org.jgroups.raft.metrics.PerformanceMetrics;

public class JGroupsRaftMetricsCollector implements JGroupsRaftMetrics {

    private final RAFT raft;
    private final ElectionMetrics electionMetrics;
    private final PerformanceMetrics performanceMetrics;
    private final LogMetrics logMetrics;

    public JGroupsRaftMetricsCollector(RAFT raft, BaseElection election, REDIRECT redirect) {
        this.raft = raft;
        this.electionMetrics = new ElectionMetricsCollector(election);
        this.performanceMetrics = new CompositePerformanceMetrics(raft, election, redirect);
        this.logMetrics = new LogMetricsCollector(raft);
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
        return logMetrics;
    }

    @Override
    public PerformanceMetrics performanceMetrics() {
        return performanceMetrics;
    }

    /**
     * Composes latency metrics by reading from each protocol's tracker at query time.
     *
     * <p>
     * Reads are lazy — each call delegates to the protocol's current tracker, so {@link org.jgroups.stack.Protocol#resetStats()}
     * on any protocol is immediately reflected.
     * </p>
     */
    private static final class CompositePerformanceMetrics implements PerformanceMetrics {
        private final RAFT raft;
        private final BaseElection election;
        private final REDIRECT redirect;

        CompositePerformanceMetrics(RAFT raft, BaseElection election, REDIRECT redirect) {
            this.raft = raft;
            this.election = election;
            this.redirect = redirect;
        }

        @Override
        public LatencyMetrics getTotalLatency() {
            return raft.totalLatencyMetrics();
        }

        @Override
        public LatencyMetrics getProcessingLatency() {
            return raft.processingLatency();
        }

        @Override
        public LatencyMetrics getLeaderElectionLatency() {
            return election.electionLatency();
        }

        @Override
        public LatencyMetrics getRedirectLatency() {
            return redirect != null ? redirect.redirectLatency() : LatencyMetrics.disabled();
        }
    }
}
