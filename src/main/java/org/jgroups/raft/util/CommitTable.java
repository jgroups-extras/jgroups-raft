package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.Log;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * The leader's knowledge of each follower's log.
 *
 * <p>
 * The leader replicates to each follower independently (Raft paper, §5.3), and what to send a follower depends on what
 * the follower answered last. Keeping that decision next to the knowledge it depends on lets the leader register
 * responses and send what the table returns, without ever changing a follower's state by itself.
 * </p>
 *
 * <p>
 * Only exists on the leader. A new leader starts with a new table.
 * </p>
 *
 * @author Bela Ban
 * @since  0.1
 */
public final class CommitTable {

    private final ConcurrentMap<Address, Entry> map = new ConcurrentHashMap<>();
    private final Log log;

    /**
     * Creates the table for the given followers.
     *
     * @param members The followers, without the leader.
     * @param log     The leader's log, which tells what each follower is missing.
     */
    public CommitTable(Collection<Address> members, Log log) {
        this.log = Objects.requireNonNull(log);
        adjust(members);
    }

    public Set<Address> keys() {
        return map.keySet();
    }

    public Entry get(Address a) {
        return map.get(a);
    }

    /**
     * Updates the followers to the given members.
     *
     * <p>
     * A new follower is assumed to have every entry until its first response says otherwise (paper, Fig. 2, nextIndex),
     * so the leader does not resend its whole log to a member that may already be up to date.
     * </p>
     *
     * @param members The followers, without the leader.
     */
    public void adjust(Collection<Address> members) {
        map.keySet().retainAll(members);
        long nextIndex = log.lastAppended() + 1;
        // entry is only created if mbr is not in map, reducing unneeded creations
        members.forEach(mbr -> map.computeIfAbsent(mbr, k -> new Entry(nextIndex)));
    }

    /**
     * Registers a follower's response to an AppendEntries request.
     *
     * <p>
     * The response tells the leader where the follower's log ends. A follower missing entries receives them right
     * away, instead of waiting for the next resend round, which only runs when the leader is idle.
     * </p>
     *
     * @param member The follower that sent the response.
     * @param result The follower's response.
     * @return What to send the follower now, {@link Send#NOTHING} if nothing.
     */
    public Send received(Address member, AppendResult result) {
        Entry e = get(member);
        if (e == null)
            return Send.NOTHING;

        return switch (result.result()) {
            case OK -> {
                e.update(result.index(), result.index() + 1, result.commitIndex());
                e.probing = false;
                if (result.index() >= e.catchUpIndex)
                    e.catchUpIndex = 0;
                yield Send.NOTHING;
            }

            case FAIL_ENTRY_NOT_FOUND -> {
                // The member's log ends at result.index(), so we don't need to probe one entry at a time.
                e.update(result.index(), result.index() + 1, result.commitIndex());
                e.probing = false;

                // Paper, Fig. 2: "If last log index >= next index for a follower"
                if (log.lastAppended() < e.nextIndex)
                    yield Send.NOTHING;

                // A rejection of a request sent before the catch-up says nothing new, the catch-up is still in flight.
                // A rejection of a request sent after it means the catch-up was lost, so it is sent again.
                if (result.prevLogIndex() < e.catchUpIndex)
                    yield Send.NOTHING;

                yield catchUp(e);
            }

            case FAIL_CONFLICTING_PREV_TERM -> {
                // The member deleted its entries from result.index(), but never the ones up to its commit index.
                e.overwrite(Math.max(result.commitIndex(), result.index() - 1), Math.max(result.commitIndex() + 1, result.index()), result.commitIndex());
                // Probing for the match point is left to the resend round, one entry at a time.
                e.probing = true;
                e.catchUpIndex = 0;
                yield Send.NOTHING;
            }
        };
    }

    /**
     * Decides what a resend round sends a follower.
     *
     * <p>
     * Raft retries requests a follower did not answer (paper, §5.3), and the resend round is that retry. A follower
     * missing entries receives them, and a follower with every entry only learns about the leader's commits.
     * </p>
     *
     * @param member The follower.
     * @return What to send the follower, {@link Send#NOTHING} if it is in sync.
     */
    public Send resend(Address member) {
        Entry e = get(member);
        if (e == null)
            return Send.NOTHING;

        // The member did not acknowledge the last catch-up, it may be unreachable.
        // A heartbeat costs a few bytes, and its answer tells where the member's log ends.
        if (e.catchUpIndex > 0)
            return new Send.Heartbeat(log.lastAppended());

        return catchUp(e);
    }

    /**
     * Decides what tells a follower about an entry the leader committed.
     *
     * <p>
     * Followers learn about commits with the next request the leader sends. Sending one right after the commit lets
     * them apply it sooner, at the cost of more messages.
     * </p>
     *
     * @param member The follower.
     * @return What to send the follower, {@link Send#NOTHING} if it has nothing new to commit.
     */
    public Send committed(Address member) {
        Entry e = get(member);
        if (e == null)
            return Send.NOTHING;

        // The follower commits at most up to the entry the heartbeat checks (paper, Fig. 2, AppendEntries RPC rule #5).
        // Checking its last acknowledged entry lets it commit what it has, while its other acknowledgements are on the way.
        if (Math.min(log.commitIndex(), e.matchIndex) <= e.commitIndex)
            return Send.NOTHING;

        // Truncated by a snapshot.
        // The resend round brings the follower up to date.
        if (e.matchIndex < log.firstAppended())
            return Send.NOTHING;

        return new Send.Heartbeat(e.matchIndex());
    }

    /**
     * Applies a function to all elements of the commit table
     */
    public void forEach(BiConsumer<Address, Entry> function) {
        for (Map.Entry<Address, Entry> entry : map.entrySet()) {
            Entry val = entry.getValue();
            function.accept(entry.getKey(), val);
        }
    }

    /**
     * Decides what the member needs next, and remembers how far it brings the member.
     *
     * @param e the entry to check
     */
    private Send catchUp(Entry e) {
        Send send = next(e);
        // The member acknowledges entries with the last index it received, and a snapshot with its last included index.
        if (send instanceof Send.Entries entries)
            e.catchUpIndex = entries.to();
        else if (send instanceof Send.Snapshot)
            e.catchUpIndex = log.firstAppended();
        return send;
    }

    private Send next(Entry e) {
        long lastAppended = log.lastAppended();
        long firstAppended = log.firstAppended();
        long commitIndex = log.commitIndex();

        // The member needs entries from nextIndex, and the AppendEntries for them checks the entry before nextIndex.
        // A snapshot truncated the log up to firstAppended, so that entry is gone, and only the snapshot can bring the member up to date.
        // A log starting at index 1 was never truncated.
        if (e.nextIndex <= firstAppended && firstAppended > 1)
            return Send.SNAPSHOT;

        // The member is missing the entries [nextIndex .. lastAppended].
        // After a conflict, the leader does not know where the member's log matches its own yet.
        // It sends one entry at a time, and each conflict moves nextIndex back, until the member accepts (paper, §5.3).
        if (lastAppended >= e.nextIndex)
            return new Send.Entries(e.nextIndex, e.probing ? e.nextIndex : lastAppended);

        // The leader already sent every entry, but the member did not acknowledge the last one.
        // A new leader is in this state, it assumes the member has every entry (paper, Fig. 2, nextIndex), and so is a member whose response was lost.
        // Resending only the last entry is enough.
        // The member either accepts it, or its rejection tells the leader where its log ends.
        if (lastAppended > e.matchIndex)
            return new Send.Entries(lastAppended, lastAppended);

        // The member has every entry, but not the leader's commit index, so it cannot apply them.
        // An AppendEntries without entries carries the commit index (paper, Fig. 2, AppendEntries RPC rule #5).
        if (commitIndex > e.commitIndex)
            return new Send.Heartbeat(lastAppended);

        // The member has every entry and the leader's commit index, but the leader has not committed all its entries.
        // A majority has them, but their acknowledgements were lost.
        // Resending them makes the member acknowledge again, so the leader can commit.
        // See https://github.com/jgroups-extras/jgroups-raft/issues/30
        if (commitIndex < lastAppended)
            return new Send.Entries(commitIndex + 1, lastAppended);

        // The member and the leader are in sync.
        return Send.NOTHING;
    }

    @Override
    public String toString() {
        return map.entrySet().stream()
                .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * A request the leader sends a follower.
     *
     * <p>
     * Sealed, so the leader handles every request the table can decide on.
     * </p>
     */
    public sealed interface Send {
        Send NOTHING = new Nothing();
        Send SNAPSHOT = new Snapshot();

        /**
         * No request, the follower is in sync.
         */
        record Nothing() implements Send { }

        /**
         * An AppendEntries without entries, for a follower that misses no entries the leader can send now.
         *
         * <p>
         * Not the periodic heartbeat from the Raft paper (§5.2), elections do not depend on it. The leader sends one when
         * the follower can learn about commits or confirm reads, or as a probe when the follower did not answer.
         * </p>
         *
         * @param prevIndex The leader's entry the follower checks, the follower commits at most up to it.
         */
        record Heartbeat(long prevIndex) implements Send { }

        /**
         * The leader's snapshot, for a follower missing entries the leader no longer has.
         */
        record Snapshot() implements Send { }

        /**
         * The entries in [from .. to], for a follower missing them.
         */
        record Entries(long from, long to) implements Send { }
    }

    /**
     * What the leader knows about a follower's log.
     *
     * <p>
     * Read-only outside the table, so only the follower's responses change it.
     * </p>
     */
    public static final class Entry {
        // The commit index of the given member.
        private long commitIndex;

        // The index of the highest entry known to be replicated to the member.
        private long matchIndex;

        // The next index to send.
        // Initialised to last_appended + 1.
        private long nextIndex;

        // A conflict moved nextIndex back.
        // The resend rounds sends a single entry until the member accepts or rejects.
        private boolean probing;

        // How far the last entries or snapshot sent to the member bring it, 0 once the member acknowledges them.
        // A request sent after them checks an entry at or after this index. When the member rejects that request, they
        // never arrived. While the member does not answer, the resend round sends heartbeats instead.
        private long catchUpIndex;

        private Entry(long nextIndex) {
            this.nextIndex = nextIndex;
        }

        public long commitIndex() {
            return commitIndex;
        }

        public long matchIndex() {
            return matchIndex;
        }

        public long nextIndex() {
            return nextIndex;
        }

        private void update(long matchIndex, long nextIndex, long commitIndex) {
            // Late responses never move what the member is known to have back.
            this.matchIndex = Math.max(matchIndex, this.matchIndex);
            this.nextIndex = Math.max(1, nextIndex);
            this.commitIndex = Math.max(this.commitIndex, commitIndex);
            assertInvariant();
        }

        private void overwrite(long matchIndex, long nextIndex, long commitIndex) {
            this.matchIndex = matchIndex;
            this.nextIndex = Math.max(1, nextIndex);
            this.commitIndex = Math.max(this.commitIndex, commitIndex);
            assertInvariant();
        }

        private void assertInvariant() {
            assert commitIndex <= matchIndex && matchIndex <= nextIndex : this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("commit-index=").append(commitIndex)
                    .append(", match-index=").append(matchIndex)
                    .append(", next-index=").append(nextIndex);
            if (probing)
                sb.append(" [probing]");
            if(catchUpIndex > 0)
                sb.append(" [catch-up=").append(catchUpIndex).append(']');
            return sb.toString();
        }
    }

}
