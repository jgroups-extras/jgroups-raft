package org.jgroups.raft.internal;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.DynamicMembership;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.JGroupsRaftAdministration;
import org.jgroups.raft.util.Utils;
import org.jgroups.util.CompletableFutures;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class JGroupsRaftAdministrationImpl implements JGroupsRaftAdministration {

    private final DynamicMembership membership;
    private final RAFT raft;
    private final BaseElection election;

    private JGroupsRaftAdministrationImpl(DynamicMembership membership, RAFT raft, BaseElection election) {
        this.membership = membership;
        this.raft = raft;
        this.election = election;
    }

    static JGroupsRaftAdministration create(JChannel channel) {
        DynamicMembership membership;
        RAFT raft;
        BaseElection election;

        // Search for the first protocol implementing membership changes.
        // The same applies as settable, we might have REDIRECT sending the commands to the correct RAFT leader.
        if ((membership = RAFT.findProtocol(DynamicMembership.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("did not find a protocol implementing DynamicMembership (e.g. REDIRECT or RAFT)");

        // Search for election protocol.
        if ((election = RAFT.findProtocol(BaseElection.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("did not find a protocol implementing BaseElection (e.g. ELECTION or ELECTION2)");

        // Search for RAFT protocol for general management.
        if ((raft = RAFT.findProtocol(RAFT.class, channel.getProtocolStack().getTopProtocol(), true)) == null)
            throw new IllegalStateException("RAFT protocol was not found");

        return new JGroupsRaftAdministrationImpl(membership, raft, election);
    }

    @Override
    public CompletionStage<String> forceLeaderElection() {
        Address leader = raft.leader();
        CompletableFuture<Address> actual = election.startForcedElection(leader)
                .toCompletableFuture();
        CompletableFuture<String> response = actual.thenApply(Utils::extractRaftId);

        // Propagate cancellation from the user-facing future back to the election future.
        // This ensures the election mechanism is cancellable/time out based on the user operation.
        response.whenComplete((ignore, t) -> {
            if (response.isCancelled()) {
                actual.cancel(true);
                return;
            }

            if (t != null)
                actual.completeExceptionally(t);
        });

        return response;
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

    @Override
    public CompletionStage<Void> snapshot() {
        return raft.snapshotAsync();
    }
}
