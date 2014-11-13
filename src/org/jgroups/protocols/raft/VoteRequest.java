package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class VoteRequest extends RaftHeader {
    public VoteRequest() {}
    public VoteRequest(int term) {super(term);}

}
