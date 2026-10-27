package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.logging.Log;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.raft.util.Utils;
import org.jgroups.util.Tuple;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Implements the behavior of a RAFT leader
 * @author Bela Ban
 * @since  0.1
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
        raft.request_table=null;
        raft.commit_table=null;

        if (reqTable != null) reqTable.destroy(raft.notCurrentLeader());
        if (raft.readOnlyRequests != null) raft.readOnlyRequests.destroy();
    }

    @Override
    public void handleAppendEntriesResponse(Address sender, long term, long round, AppendResult result) {
        RequestTable<String> reqtab=raft.request_table;
        if(reqtab == null)
            throw new IllegalStateException("request table cannot be null in leader");
        String sender_raft_id=Utils.extractRaftId(sender);
        Log log=raft.getLog();
        if(log.isTraceEnabled())
            log.trace("%s: received AppendEntries response from %s for term %d: %s", raft.getAddress(), sender, term, result);

        // Any response received in this term proves the sender still follows this leader, even failures confirm reads, too.
        // Responses from older terms never reach this point, the up handler drops them.
        if (Utils.isRaftMember(sender_raft_id, raft.members())) {
            raft.readOnlyRequests.confirm(round, sender_raft_id, raft.commit_index);
        }

        switch(result.result) {
            case OK:
                raft.commit_table.update(sender, result.index(), result.index() + 1, result.commit_index, false);
                catchingUp.remove(sender);

                // Learner members do not count to an entry commit.
                if (!Utils.isRaftMember(sender_raft_id, raft.members()))
                    break;

                boolean done = reqtab.add(result.index, sender_raft_id, this.majority);

                // Raft paper, Fig. 2, Rules for Servers, Leaders point #4.
                // If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm,
                // set commitIndex = N.
                // Entries from previous terms are committed together with an entry of this term, never by only counting
                // the members that store them (paper §5.4.2, Figure 8).
                if (done &&isFromCurrentTerm(result.index)) {
                    raft.commitLogTo(result.index, true);
                }

                // Send commits immediately.
                // Note that, an entry is committed by a MAJORITY, this means that some of the nodes doesn't know the entry exist yet.
                // This way, send the commit messages any time we handle an append response.
                if (raft.send_commits_immediately && (done || reqtab.isCommitted(result.index))) {
                    // Done is only true when reaching a majority threshold, we also need to check is committed to resend
                    // to slower nodes.
                    sendCommitMessageToFollowers();
                }
                break;
            case FAIL_ENTRY_NOT_FOUND:
                // The follower's log is shorter than prev_index and result.index() is its last appended entry.
                // We know where its log ends, so we don't need to probe one entry at a time.
                // The next resend carries [next_index .. last_appended] (Paper, Fig. 2).
                // The follower still checks prev_index/prev_term on that resend.
                // If the term at its last entry conflicts with ours, it answers FAIL_CONFLICTING_PREV_TERM, which moves to single-entry probing.
                raft.commit_table.update(sender, result.index(), result.index() + 1, result.commitIndex(), false);

                // Figure 2 in paper: "If last log index >= next index for a follower".
                // We retry now instead of waiting for the next resend round, which only runs with `resend_interval` when idle.
                // Under constant writes, that can take a long time, and the follower keeps rejecting every multicast.
                CommitTable.Entry entry = raft.commit_table.get(sender);
                if (entry != null && raft.lastAppended() >= entry.nextIndex() && catchingUp.add(sender))
                    raft.sendAppendEntriesMessage(sender, entry);
                break;
            case FAIL_CONFLICTING_PREV_TERM:
                long matchIndex = Math.max(result.commit_index, result.index() - 1);
                long nextIndex = Math.max(result.commit_index + 1, result.index());
                raft.commit_table.update(sender, matchIndex, nextIndex, result.commit_index, true, true);
                // The member answered.
                // Probing for the match point is left to what was resent.
                catchingUp.remove(sender);
                break;
        }
    }

    private void sendCommitMessageToFollowers() {
        raft.commit_table.forEach(this::sendCommitMessageToFollower);
    }

    private void sendCommitMessageToFollower(Address member, CommitTable.Entry entry) {
        // The follower commits at most up to the last entry if verified against the leader's log.
        // This is the rule from raft paper, Fig. 2, AppendEntries RPC, rule #5.
        // We send our entry at its match index, so the message only helps when that is above its commit index.
        long matchIndex = entry.matchIndex();
        if (Math.min(raft.commit_index, matchIndex) <= entry.commitIndex())
            return;

        LogEntry prev = raft.log_impl.get(matchIndex);
        if (prev == null) {
            // Truncated by a snapshot.
            // The resend round takes care of this member.
            return;
        }

        Tuple<Address, Long> tuple = raft.leaderAndTerm();
        Address leader = tuple.val1();
        long cterm = tuple.val2();
        short id = raft.getId();
        Message msg=new ObjectMessage(member, null)
                .putHeader(id, new AppendEntriesRequest(leader, cterm, matchIndex, prev.term(), cterm, raft.commit_index));
        raft.down(msg);
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
