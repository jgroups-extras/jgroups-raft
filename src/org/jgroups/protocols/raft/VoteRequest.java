package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class VoteRequest extends RaftHeader {
    public VoteRequest() {}
    public VoteRequest(int term) {super(term);}

}
