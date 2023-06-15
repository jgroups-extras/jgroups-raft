package org.jgroups.protocols.raft.election;

import org.jgroups.Header;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.RaftHeader;

import java.util.function.Supplier;

/**
 * Utilized during the pre-voting phase to ask nodes information about their leader.
 *
 * @author José Bolina
 * @since 1.0.12
 */
public class PreVoteRequest extends RaftHeader {

    public PreVoteRequest() { }

    @Override
    public short getMagicId() {
        return ELECTION2.PRE_VOTE_REQ;
    }

    @Override
    public Supplier<? extends Header> create() {
        return PreVoteRequest::new;
    }

    @Override
    public String toString() {
        return "PreVote: " + super.toString();
    }
}
