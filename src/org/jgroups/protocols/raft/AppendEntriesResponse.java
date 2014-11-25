package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class AppendEntriesResponse extends RaftHeader {
    public AppendEntriesResponse() {}
    public AppendEntriesResponse(int term) {super(term);}
}
