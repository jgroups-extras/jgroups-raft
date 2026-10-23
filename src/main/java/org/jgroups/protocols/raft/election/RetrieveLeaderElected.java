package org.jgroups.protocols.raft.election;

import static org.jgroups.protocols.raft.election.BaseElection.RETRIEVE_LEADER_ELECTED;

import org.jgroups.Header;
import org.jgroups.protocols.raft.RaftHeader;

import java.util.function.Supplier;

/**
 * Asks the view coordinator for the elected leader and its term.
 *
 * <p>
 * Sent by a member that may have missed the leader's announcement, e.g., one that installed its Raft stack on a fork
 * channel after joining the cluster. The coordinator answers with a {@link LeaderElected}.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see BaseElection
 */
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
