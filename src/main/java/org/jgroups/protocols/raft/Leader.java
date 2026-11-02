package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.raft.util.Utils;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Implements the behavior of a RAFT leader
 *
 * @author Bela Ban
 * @since 0.1
 */
public class Leader extends RaftImpl {
    protected final Supplier<Integer> majority = raft::majority;

    // Members that are delayed and currently catching up (FAIL_ENTRY_NOT_FOUND).
    // A member stays here at most until its next response.
    // The idea is to avoid resending a batch of entries that was already retransmitted.
    private final Set<Address> catchingUp = ConcurrentHashMap.newKeySet();

    public Leader(RAFT raft) {
        super(raft);
    }

    @Override
    public void init() {
        super.init();
        raft.createRequestTable();
        raft.createCommitTable();
        raft.createReadOnlyRequestRepository();
    }

    @Override
    public void destroy() {
        super.destroy();
        RequestTable<String> reqTable = raft.request_table;
        raft.request_table = null;
        raft.commit_table = null;

        if (reqTable != null) reqTable.destroy(raft.notCurrentLeader());
        if (raft.readOnlyRequests != null) raft.readOnlyRequests.destroy();
    }

    @Override
    public void handleAppendEntriesResponse(Address sender, long term, long round, AppendResult result) {
        RequestTable<String> reqtab = raft.request_table;
        if (reqtab == null)
            throw new IllegalStateException("request table cannot be null in leader");
        String senderRaftId = Utils.extractRaftId(sender);
        Log log = raft.getLog();
        if (log.isTraceEnabled())
            log.trace("%s: received AppendEntries response from %s for term %d: %s", raft.getAddress(), sender, term, result);

        // Any response received in this term proves the sender still follows this leader, even failures confirm reads, too.
        // Responses from older terms never reach this point, the up handler drops them.
        if (Utils.isRaftMember(senderRaftId, raft.members())) {
            raft.readOnlyRequests.confirm(round, senderRaftId, raft.commit_index);
        }

        // The table registers where the member's log ends and decides what it needs next.
        // For example, a member that rejected an entry it does not have receives the missing entries right away.
        CommitTable.Send send = raft.commit_table.received(sender, result);

        // In case of success, handle the commit response.
        if (result.success())
            commit(senderRaftId, result.index());

        // Handle the callback acting on the response of the node.
        raft.send(sender, send);
    }

    private void commit(String senderRaftId, long index) {
        // Learner members do not count to an entry commit.
        if (!Utils.isRaftMember(senderRaftId, raft.members()))
            return;

        RequestTable<String> rt = raft.request_table;
        boolean done = rt.add(index, senderRaftId, this.majority);

        // Raft paper, Fig. 2, Rules for Servers, Leaders point #4.
        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N.
        // Entries from previous terms are committed together with an entry of this term, never by only counting the members that store them (paper §5.4.2, Figure 8).
        if (done && isFromCurrentTerm(index)) {
            raft.commitLogTo(index, true);
        }

        // Send commits immediately.
        // Note that, an entry is committed by a MAJORITY, this means that some of the nodes doesn't know the entry exist yet.
        // This way, send the commit messages any time we handle an append response.
        if (raft.send_commits_immediately && (done || rt.isCommitted(index))) {
            // Done is only true when reaching a majority threshold
            // We also need to check is committed to resend to slower nodes that commit later.
            sendCommitMessageToFollowers();
        }
    }

    private void sendCommitMessageToFollowers() {
        raft.commit_table.forEach((member, ignore) -> raft.send(member, raft.commit_table.committed(member)));
    }

    /**
     * Checks whether the entry at the given index was created in the current term.
     *
     * @param index the index of an entry in the leader's log.
     * @return {@code true} if the entry exists and belongs to the current term, {@code false} otherwise.
     */
    private boolean isFromCurrentTerm(long index) {
        LogEntry entry = raft.log_impl.get(index);
        return entry != null && entry.term() == raft.currentTerm();
    }
}
