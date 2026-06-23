package org.jgroups.protocols.raft.internal.snapshot;

public final class SnapshotConstant {

    private SnapshotConstant() { }

    public static final short SNAPSHOT_METADATA_REQ = 2005;
    public static final short SNAPSHOT_CHUNK_REQ = 2006;
    public static final short SNAPSHOT_CHUNK_RSP = 2007;
}
