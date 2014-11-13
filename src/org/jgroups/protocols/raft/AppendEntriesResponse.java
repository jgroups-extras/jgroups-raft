package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class AppendEntriesResponse extends RaftHeader {
    public AppendEntriesResponse() {}
    public AppendEntriesResponse(int term) {super(term);}
}
