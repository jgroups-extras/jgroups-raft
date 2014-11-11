package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class InstallSnapshotHeader extends RaftHeader {
    public InstallSnapshotHeader(int term) {super(term);}
}
