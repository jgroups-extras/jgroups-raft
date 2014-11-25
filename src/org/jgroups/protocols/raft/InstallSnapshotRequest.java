package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class InstallSnapshotRequest extends RaftHeader {
    public InstallSnapshotRequest() {}
    public InstallSnapshotRequest(int term) {super(term);}
}
