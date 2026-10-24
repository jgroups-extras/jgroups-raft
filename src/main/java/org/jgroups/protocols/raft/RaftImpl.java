package org.jgroups.protocols.raft;

import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_CONFLICTING_PREV_TERM;
import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_ENTRY_NOT_FOUND;
import static org.jgroups.protocols.raft.AppendResult.Result.OK;

import org.jgroups.Address;
import org.jgroups.Message;

/**
 * Base class for the role this node has in the Raft protocol: {@link Follower}, {@link Learner} or {@link Leader}.
 *
 * <p>
 * {@link RAFT} creates a new instance whenever the role changes and hands it the requests and responses it receives.
 * Each role applies the rules that Figure 2 of the Raft paper defines for it
 * (<a href="https://raft.github.io/raft.pdf">In Search of an Understandable Consensus Algorithm, extended version</a>).
 * Any role can receive an AppendEntries request, so this class implements the receiver side of AppendEntries.
 * Subclasses override the hooks for the messages only their role handles, such as AppendEntries responses on the
 * leader and InstallSnapshot requests on the followers. Elections are not handled here, but by the election protocol.
 * </p>
 *
 * @author Bela Ban
 * @see <a href="https://raft.github.io/raft.pdf">In Search of an Understandable Consensus Algorithm, extended version</a>
 * @since 0.1
 */
public abstract class RaftImpl {
    protected final RAFT raft; // a ref to the enclosing RAFT protocol

    public RaftImpl(RAFT raft) {
        this.raft = raft;
    }

    public final RAFT raft() {
        return raft;
    }

    /**
     * Prepares this role after {@link RAFT} switches to it, before it receives any message.
     */
    public void init() { }

    /**
     * Releases the resources of this role when {@link RAFT} switches to another role. The instance receives no more
     * messages afterwards.
     */
    public void destroy() { }

    /**
     * Handles an AppendEntries request from the leader, following the receiver implementation in Figure 2 of the Raft
     * paper.
     *
     * <p>
     * The request is ignored when this node does not accept the sender as the leader of {@code messageTerm}, for example
     * because it already knows a higher term (rule 1). Otherwise, the request succeeds only if the log holds an entry at
     * {@code prev_index} with term {@code prev_term} (rule 2). Committed entries always pass this check: every leader
     * has them in its log (Leader Completeness, §5.4). On success, an entry whose term differs from the one in the
     * request is replaced together with all the entries after it (rule 3), and the entries missing from the log are
     * appended (rule 4). Then the commit index moves up to the lower of {@code leader_commit} and the index of the last
     * entry in the request (rule 5).
     * </p>
     *
     * <p>
     * A request without entries follows the same rules. It lets this node commit up to {@code prev_index} at most,
     * because the entries after it were not checked against the leader's log.
     * </p>
     *
     * <p>
     * A successful result carries the index of the last entry in the request. The leader counts the response towards
     * the commit of every entry up to that index, so the index never covers entries the request did not check.
     * A failed result tells the leader where to continue: the last index in the log when no entry exists at
     * {@code prev_index}, or the first index of the conflicting term when the terms differ (§5.3).
     * </p>
     *
     * @param entries       the entries following {@code prev_index}, or {@code null} or empty when the request only carries
     *                      the leader's commit index.
     * @param leader        the leader that sent the request.
     * @param messageTerm   the leader's current term.
     * @param prev_index    the index of the entry preceding {@code entries} (prevLogIndex).
     * @param prev_term     the term of the entry at {@code prev_index} (prevLogTerm).
     * @param entry_term    each entry carries its own term.
     * @param leader_commit the leader's commit index (leaderCommit).
     * @return the result to send back to the leader, or {@code null} when the request is ignored.
     */
    public final AppendResult handleAppendEntriesRequest(LogEntries entries, Address leader, long messageTerm,
                                                         long prev_index, long prev_term, long entry_term, long leader_commit) {
        if (raft.trySetLeaderAndTerm(leader, messageTerm) < 0) {
            // There was a change in leader while handling this request.
            // It could be a new leader was elected or that this node received a vote request with higher term.
            // We skip this entry altogether. The new leader would replicate it again.
            return null;
        }

        // Consistency check (paper, Fig 2. AppendEntries RPC rule #2).
        // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if (prev_index > raft.commitIndex()) {
            LogEntry prev = raft.log_impl.get(prev_index);
            if (prev == null) {
                raft.num_failed_append_requests_not_found++;
                return new AppendResult(FAIL_ENTRY_NOT_FOUND, raft.lastAppended());
            }

            if (prev.term != prev_term) {
                // Return false and the start index of the conflicting term.
                raft.num_failed_append_requests_wrong_term++;
                long conflictingIndex = getFirstIndexOfConflictingTerm(prev_index, prev.term);
                if (conflictingIndex <= raft.commitIndex()) {
                    raft.getLog().error("%s: cannot delete entries <= %d as commit_index is higher: log=%s",
                            raft.getAddress(), conflictingIndex, raft.log_impl);
                    conflictingIndex = raft.last_appended;
                } else {
                    raft.deleteAllLogEntriesStartingFrom(conflictingIndex);
                }
                return new AppendResult(FAIL_CONFLICTING_PREV_TERM, conflictingIndex, prev.term)
                        .commitIndex(raft.commitIndex());
            }
        }

        // The log matches the leader's up to prev_index.
        // Raft paper, Fig. 2, AppendEntries RPC:
        // Rule #3: If an existing entry conflicts with a new one (equals indexes, different terms), delete the existing
        //          and all that follows it.
        // Rule #4: Append any new entries not already in the log.
        int numEntries = entries == null ? 0 : entries.size();
        long index = prev_index;
        long appendFrom = 0;
        LogEntries toAppend = null;
        for (int i = 0; i < numEntries; i++) {
            LogEntry entry = entries.get(i);
            index++;
            if (toAppend == null) {
                if (index <= raft.commitIndex()) {
                    // Processing a batch that was previously committed, just continue.
                    continue;
                }

                LogEntry existing = raft.log_impl.get(index);
                // Entries in the same index with matching term, just continue
                if (existing != null && existing.term == entry.term)
                    continue;

                // Entry in the same index with different term, rule #3 applies.
                if (existing != null)
                    raft.deleteAllLogEntriesStartingFrom(index);

                toAppend = new LogEntries();
                appendFrom = index;
            }

            // Collect which entries are not already in the log.
            toAppend.add(entry);
        }

        if (toAppend != null) {
            raft.append(appendFrom, toAppend);
            raft.num_successful_append_requests += toAppend.size();
        }

        // Entries to commit are collected.
        // Raft paper, Fig. 2, AppendEntries RPC.
        // Rule #5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        long lastNewIndex = prev_index + numEntries;
        raft.commitLogTo(Math.min(leader_commit, lastNewIndex), false);
        // Report what we verified instead of retrieving lastAppended()
        return new AppendResult(OK, lastNewIndex)
                .commitIndex(raft.commitIndex());
    }

    /**
     * Handles a follower's response to an AppendEntries request. Only the leader acts on it: it updates what it knows
     * about the follower's log and retries or commits accordingly (Figure 2, Rules for Servers, Leaders). The other
     * roles ignore it.
     *
     * @param sender the member that sent the response.
     * @param term   the sender's current term.
     * @param result the outcome of the request on the sender.
     */
    public void handleAppendEntriesResponse(Address sender, long term, AppendResult result) { }

    /**
     * Handles an InstallSnapshot request from the leader (Raft paper, §7 and Figure 13). The leader sends one when the
     * entries a follower needs are no longer in its log because a snapshot replaced them.
     *
     * <p>
     * The follower replaces its state with the snapshot and resets its log to end at the last index the snapshot
     * includes, with the term of that entry. The next AppendEntries request can then check its {@code prev_index}
     * against that entry. Only the follower roles act on this request.
     * </p>
     *
     * @param msg    the message carrying the snapshot.
     * @param hdr    the header describing the snapshot.
     * @param leader the leader that sent the snapshot.
     */
    public void handleInstallSnapshotRequest(Message msg, RaftHeader hdr, Address leader) { }

    /**
     * Finds the first index of the term held at {@code start_index}, searching backwards from {@code start_index}
     * and staying above the commit index. The leader uses it to skip all the entries of that term in one round trip
     * instead of one entry per round trip (§5.3).
     *
     * @param start_index      the index whose entry conflicts with the leader's log.
     * @param conflicting_term the term of the entry at {@code start_index} in this log.
     * @return the first index above the commit index holding {@code conflicting_term}, or {@code start_index} capped
     * at the last index in the log.
     */
    private long getFirstIndexOfConflictingTerm(long start_index, long conflicting_term) {
        RaftLogAdapter log = raft.log_impl;
        long first = Math.max(1, log.firstAppended()), last = log.lastAppended(), commit_index = log.commitIndex();
        long retval = Math.min(start_index, last);
        for (long i = retval; i >= first && i > commit_index; i--) {
            LogEntry entry = log.get(i);
            if (entry == null || entry.term != conflicting_term)
                break;
            retval = i;
        }
        return retval;
    }
}
