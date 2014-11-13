package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class InstallSnapshotResponse extends RaftHeader {
    public InstallSnapshotResponse() {}
    public InstallSnapshotResponse(int term) {super(term);}
}
