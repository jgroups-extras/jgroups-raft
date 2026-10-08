package org.jgroups.protocols.raft.election;

import static org.jgroups.protocols.raft.election.BaseElection.RETRIEVE_LEADER_ELECTED;

import org.jgroups.Header;
import org.jgroups.protocols.raft.RaftHeader;

import java.util.function.Supplier;

public class RetrieveLeaderElected extends RaftHeader {

    @Override
    public short getMagicId() {
        return RETRIEVE_LEADER_ELECTED;
    }

    @Override
    public Supplier<? extends Header> create() {
        return RetrieveLeaderElected::new;
    }
}
