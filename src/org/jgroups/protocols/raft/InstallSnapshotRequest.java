package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class InstallSnapshotRequest extends RaftHeader {
    public InstallSnapshotRequest() {}
    public InstallSnapshotRequest(int term) {super(term);}
}
