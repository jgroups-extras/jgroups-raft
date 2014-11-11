package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class AppendEntriesHeader extends RaftHeader {

    public AppendEntriesHeader(int term) {super(term);}
}
