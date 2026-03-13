package org.jgroups.raft.internal.metrics;

import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.metrics.LogMetrics;

/**
 * Reads log state lazily from the RAFT protocol.
 *
 * <p>
 * All methods delegate to the protocol at query time, so values always reflect the current state.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class LogMetricsCollector implements LogMetrics {
    private final RAFT raft;
    private final Log log;

    LogMetricsCollector(RAFT raft) {
        this.raft = raft;
        this.log = raft.log();
    }

    @Override
    public long getTotalLogEntries() {
        return log.size();
    }

    @Override
    public long getCommittedLogEntries() {
        long first = log.firstAppended();
        long commit = log.commitIndex();
        if (commit == 0)
            return 0L;
        return first == 0 ? commit : commit - first + 1;
    }

    @Override
    public long getUncommittedLogEntries() {
        return log.lastAppended() - log.commitIndex();
    }

    @Override
    public long getLogSizeInBytes() {
        return log.sizeInBytes();
    }

    @Override
    public long getCurrentTerm() {
        return log.currentTerm();
    }

    @Override
    public long getCommitIndex() {
        return log.commitIndex();
    }

    @Override
    public int getSnapshotCount() {
        return raft.numSnapshots();
    }

    @Override
    public int getSnapshotsReceived() {
        return raft.numSnapshotReceived();
    }

    @Override
    public String toString() {
        return "LogMetricsCollector[raft=" + raft + ']';
    }

}
