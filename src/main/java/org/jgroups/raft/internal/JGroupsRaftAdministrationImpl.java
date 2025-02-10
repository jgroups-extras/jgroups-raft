package org.jgroups.raft.internal;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.DynamicMembership;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.JGroupsRaftAdministration;
import org.jgroups.util.CompletableFutures;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class JGroupsRaftAdministrationImpl implements JGroupsRaftAdministration {

    private final DynamicMembership membership;
    private final BaseElection election;
    private final RAFT raft;

    private JGroupsRaftAdministrationImpl(DynamicMembership membership, BaseElection election, RAFT raft) {
        this.membership = membership;
        this.election = election;
        this.raft = raft;
    }

    static JGroupsRaftAdministration create(JChannel channel) {
        DynamicMembership membership;
        BaseElection election;
        RAFT raft;

        // Search for the first protocol implementing membership changes.
        // The same applies as settable, we might have REDIRECT sending the commands to the correct RAFT leader.
        if ((membership = RAFT.findProtocol(DynamicMembership.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("did not find a protocol implementing DynamicMembership (e.g. REDIRECT or RAFT)");

        // Search for election protocol.
        if ((election = RAFT.findProtocol(BaseElection.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("did not find a protocol implementing DynamicMembership (e.g. REDIRECT or RAFT)");

        // Search for RAFT protocol for general management.
        if ((raft = RAFT.findProtocol(RAFT.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("RAFT protocol was not found");

        return new JGroupsRaftAdministrationImpl(membership, election, raft);
    }

    @Override
    public CompletionStage<String> forceLeaderElection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<Void> addNode(String raftId) {
        try {
            return membership.addServer(raftId).thenApply(CompletableFutures.toVoidFunction());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<Void> removeNode(String raftId) {
        try {
            return membership.removeServer(raftId).thenApply(CompletableFutures.toVoidFunction());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public Set<String> members() {
        return Set.copyOf(raft.members());
    }
}
