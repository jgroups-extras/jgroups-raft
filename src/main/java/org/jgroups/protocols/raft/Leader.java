package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.logging.Log;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.raft.util.Utils;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Implements the behavior of a RAFT leader
 * @author Bela Ban
 * @since  0.1
 */
public class Leader extends RaftImpl {
    protected final Supplier<Integer> majority=() -> raft.majority();

    // Members that are delayed and currently catching up (FAIL_ENTRY_NOT_FOUND).
    // A member stays here at most until its next response.
    // The idea is to avoid resending a batch of entries that was already retransmitted.
    private final Set<Address> catchingUp = ConcurrentHashMap.newKeySet();

    public Leader(RAFT raft) {
        super(raft);
    }

    public void init() {
        super.init();
        raft.createRequestTable();
        raft.createCommitTable();
        raft.createReadOnlyRequestRepository();
    }

    public void destroy() {
        super.destroy();
        RequestTable<String> reqTable = raft.request_table;
        raft.request_table=null;
        raft.commit_table=null;

        if (reqTable != null) reqTable.destroy(raft.notCurrentLeader());
        if (raft.readOnlyRequests != null) raft.readOnlyRequests.destroy();
    }


    @Override
    public void handleAppendEntriesResponse(Address sender, long term, AppendResult result) {
        RequestTable<String> reqtab=raft.request_table;
        if(reqtab == null)
            throw new IllegalStateException("request table cannot be null in leader");
        String sender_raft_id=Utils.extractRaftId(sender);
        Log log=raft.getLog();
        if(log.isTraceEnabled())
            log.trace("%s: received AppendEntries response from %s for term %d: %s", raft.getAddress(), sender, term, result);
        switch(result.result) {
            case OK:
                raft.commit_table.update(sender, result.index(), result.index() + 1, result.commit_index, false);
                catchingUp.remove(sender);

                // Learner members do not count to an entry commit.
                if (!Utils.isRaftMember(sender_raft_id, raft.members()))
                    break;

                raft.readOnlyRequests.commit(raft.commit_index, sender_raft_id);
                boolean done = reqtab.add(result.index, sender_raft_id, this.majority);
                if(done) {
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
        if(raft.commit_index > entry.commitIndex()) {
            long cterm=raft.currentTerm();
            short id=raft.getId();
            Address leader=raft.getAddress();
            Message msg=new ObjectMessage(member, null)
              .putHeader(id, new AppendEntriesRequest(leader, cterm, 0, 0, cterm, raft.commit_index));
            raft.down(msg);
        }
    }
}
