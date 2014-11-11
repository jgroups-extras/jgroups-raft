package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class RequestVoteResponse extends RaftHeader {

    public RequestVoteResponse(int term) {super(term);}

}
