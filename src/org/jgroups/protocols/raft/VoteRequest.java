package org.jgroups.protocols.raft;

import org.jgroups.Header;

import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class VoteRequest extends RaftHeader {

    public VoteRequest() {}
    public VoteRequest(int term) {
        super(term);
    }

    public short getMagicId() {
        return ELECTION.VOTE_REQ;
    }

    public Supplier<? extends Header> create() {
        return VoteRequest::new;
    }

}
