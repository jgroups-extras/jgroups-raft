package org.jgroups.protocols.raft.election;

import org.jgroups.Header;
import org.jgroups.protocols.raft.RaftHeader;

import java.util.function.Supplier;

import static org.jgroups.protocols.raft.election.BaseElection.VOTE_REQ;

/**
 * @author Bela Ban
 * @since  0.1
 */
public class VoteRequest extends RaftHeader {

    public VoteRequest() {}
    public VoteRequest(long term) {
        super(term);
    }

    public short getMagicId() {
        return VOTE_REQ;
    }

    public Supplier<? extends Header> create() {
        return VoteRequest::new;
    }

}
