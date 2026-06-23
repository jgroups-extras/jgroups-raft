package org.jgroups.protocols.raft.internal.snapshot;

import org.jgroups.raft.util.TimeService;

final class DefaultSnapshotMetrics implements SnapshotMetrics {

    private final TimeService timeService;

    private int numSnapshots;
    private int numSnapshotsReceived;
    private int numFailedSnapshots;
    private int numFailedInstalls;
    private int numChunksReceived;
    private long numBytesReceived;
    private int numFailedChunkTransfers;
    private long totalChunkIntervalNs;
    private long lastChunkTimestamp;
    private long transferStartTimestamp;
    private long lastTransferDurationNs;

    DefaultSnapshotMetrics(TimeService timeService) {
        this.timeService = timeService;
    }

    @Override
    public int numSnapshots() {
        return numSnapshots;
    }

    @Override
    public int numSnapshotsReceived() {
        return numSnapshotsReceived;
    }

    @Override
    public int numFailedSnapshotsTaken() {
        return numFailedSnapshots;
    }

    @Override
    public int numFailedSnapshotsInstalled() {
        return numFailedInstalls;
    }

    @Override
    public int numChunksReceived() {
        return numChunksReceived;
    }

    @Override
    public long numBytesReceived() {
        return numBytesReceived;
    }

    @Override
    public int numFailedChunkTransfers() {
        return numFailedChunkTransfers;
    }

    @Override
    public long avgChunkIntervalNanos() {
        return numChunksReceived > 1 ? totalChunkIntervalNs / (numChunksReceived - 1) : 0;
    }

    @Override
    public long lastTransferDurationNanos() {
        return lastTransferDurationNs;
    }

    @Override
    public void reset() {
        numSnapshots = 0;
        numSnapshotsReceived = 0;
        numFailedSnapshots = 0;
        numFailedInstalls = 0;
        numChunksReceived = 0;
        numBytesReceived = 0;
        numFailedChunkTransfers = 0;
        totalChunkIntervalNs = 0;
        lastChunkTimestamp = 0;
        transferStartTimestamp = 0;
        lastTransferDurationNs = 0;
    }

    void snapshotCreated() {
        numSnapshots++;
    }

    void snapshotFailedCreate() {
        numFailedSnapshots++;
    }

    void snapshotReceived() {
        numSnapshotsReceived++;
    }

    void snapshotFailedInstall() {
        numFailedInstalls++;
    }

    void chunkTransferStarted() {
        transferStartTimestamp = timeService.nanos();
        lastChunkTimestamp = transferStartTimestamp;
    }

    void chunkReceived(long bytes) {
        long now = timeService.nanos();
        if (numChunksReceived > 0) {
            totalChunkIntervalNs += now - lastChunkTimestamp;
        }

        lastChunkTimestamp = now;
        numChunksReceived++;
        numBytesReceived += bytes;
    }

    void chunkTransferCompleted() {
        lastTransferDurationNs = timeService.interval(transferStartTimestamp);
    }

    void chunkTransferFailed() {
        numFailedChunkTransfers++;
    }
}
