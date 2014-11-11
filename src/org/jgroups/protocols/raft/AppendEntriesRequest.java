package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class AppendEntriesRequest extends RaftHeader {

    public AppendEntriesRequest(int term) {super(term);}
}
