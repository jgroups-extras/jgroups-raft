package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class InstallSnapshotResponse extends RaftHeader {
    public InstallSnapshotResponse() {}
    public InstallSnapshotResponse(int term) {super(term);}
}
