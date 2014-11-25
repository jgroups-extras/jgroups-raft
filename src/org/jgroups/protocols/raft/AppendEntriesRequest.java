package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class AppendEntriesRequest extends RaftHeader {
    public AppendEntriesRequest() {}
    public AppendEntriesRequest(int term) {super(term);}
}
