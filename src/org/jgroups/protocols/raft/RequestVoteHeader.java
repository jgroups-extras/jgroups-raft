package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  3.6
 */
public class RequestVoteHeader extends RaftHeader {

    public RequestVoteHeader(int term) {super(term);}

}
