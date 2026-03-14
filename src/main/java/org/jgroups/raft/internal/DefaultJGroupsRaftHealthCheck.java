package org.jgroups.raft.internal;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaftHealthCheck;
import org.jgroups.raft.util.Utils;

import java.util.Collection;

final class DefaultJGroupsRaftHealthCheck implements JGroupsRaftHealthCheck {

    private final JChannel channel;
    private RAFT raft;

    DefaultJGroupsRaftHealthCheck(JChannel channel) {
        this.channel = channel;
    }

    void start(RAFT raft) {
        this.raft = raft;
    }

    void stop() {
        this.raft = null;
    }

    @Override
    public boolean isNodeLive() {
        return channel.isConnected() && !channel.isClosed();
    }

    @Override
    public boolean isNodeReady() {
        return isNodeLive() && raft != null && raft.leader() != null;
    }

    @Override
    public ClusterHealth getClusterHealth() {
        if (!channel.isConnected() || raft == null)
            return ClusterHealth.NOT_RUNNING;

        if (raft.leader() == null)
            return ClusterHealth.FAILURE;

        // We count only the voting members and skip learners: they do not count for consensus.
        int votingMembers = countActiveVotingMembers();
        int totalMembers = raft.members().size();

        // This is a double guard.
        // We shouldn't have a leader if this check is true, but better be safe.
        if (votingMembers < raft.majority())
            return ClusterHealth.FAILURE;

        // If we have missing voting members, we are in degraded mode.
        // For example, we have the Raft membership of 5 nodes, we have the 3 for quorum, but we are missing 2 nodes.
        if (votingMembers < totalMembers)
            return ClusterHealth.DEGRADED;

        return ClusterHealth.HEALTHY;
    }

    /**
     * Counts how many voting Raft members are present in the current JGroups view.
     * Excludes learners (nodes connected but not in the Raft membership).
     */
    private int countActiveVotingMembers() {
        Collection<String> raftMembers = raft.members();
        View view = channel.getView();
        int count = 0;
        for (Address addr : view.getMembersRaw()) {
            String raftId = Utils.extractRaftId(addr);
            if (raftMembers.contains(raftId))
                count++;
        }
        return count;
    }
}
