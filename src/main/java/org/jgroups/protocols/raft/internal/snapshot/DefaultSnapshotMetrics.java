package org.jgroups.protocols.raft.internal.snapshot;

final class DefaultSnapshotMetrics implements SnapshotMetrics {

    private int numSnapshots;
    private int numSnapshotsReceived;
    private int numFailedSnapshots;
    private int numFailedInstalls;

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
    public void reset() {
        numSnapshots = 0;
        numSnapshotsReceived = 0;
        numFailedSnapshots = 0;
        numFailedInstalls = 0;
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
}
