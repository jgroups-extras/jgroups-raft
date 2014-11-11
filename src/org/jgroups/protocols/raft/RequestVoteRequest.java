package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class RequestVoteRequest extends RaftHeader {

    public RequestVoteRequest(int term) {super(term);}

}
