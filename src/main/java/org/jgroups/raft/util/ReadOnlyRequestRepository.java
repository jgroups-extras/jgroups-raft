package org.jgroups.raft.util;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Repository for read-only operations.
 *
 * <p>
 * Read-only operations do not go through the log. The leader serves a read once it confirms it is still the leader
 * and its commit index reaches the read index (Raft dissertation, §6.4). Leadership is confirmed by a majority
 * answering a request the leader sent after the read arrived. Requests carry a round number which the response
 * echoes, so the leader knows which request a response answers.
 * </p>
 *
 * <p>
 * Reads waiting for the same request share a round and complete together.
 * </p>
 *
 * @param <R> The type of requests.
 * @author José Bolina
 * @since 1.1.2
 */
public final class ReadOnlyRequestRepository<R> {
    private static final Log LOG = LogFactory.getLog(ReadOnlyRequestRepository.class);

    private final NavigableMap<Long, Entry> requests = new TreeMap<>(Long::compareTo);
    private final Supplier<Integer> majority;
    private final Consumer<Collection<R>> listener;
    private final Consumer<Collection<R>> destroyer;
    private final long minimumIndex;

    // Round 0 is never handed out, so a response without a round never confirms a read.
    private long round;
    private boolean destroyed;

    private ReadOnlyRequestRepository(long minimumIndex, Supplier<Integer> majority, Consumer<Collection<R>> listener, Consumer<Collection<R>> destroyer) {
        this.minimumIndex = minimumIndex;
        this.majority = majority;
        this.listener = listener;
        this.destroyer = destroyer;
        this.destroyed = false;
    }

    /**
     * Invokes the registered destroyer with all pending requests.
     *
     * <p>
     * Subsequent calls to register operations after destroy will automatically invoke the destroyer listener.
     * </p>
     */
    public void destroy() {
        destroyed = true;
        for (Entry value : requests.values()) {
            destroyer.accept(value.requests);
        }
        requests.clear();
    }

    /**
     * Completes the reads confirmed by a majority with a read index up to the given commit index.
     *
     * @param index The leader's current commit index.
     */
    public void advance(long index) {
        Iterator<Entry> it = requests.values().iterator();
        while (it.hasNext()) {
            Entry entry = it.next();
            if (entry.isConfirmed() && entry.readIndex <= index) {
                listener.accept(entry.requests);
                it.remove();
            }
        }
    }

    /**
     * Registers a follower's response to a request carrying the given round.
     *
     * <p>
     * The response proves the follower still accepted this leader after the reads of that round, and of every earlier
     * round, were registered (Raft dissertation, §6.4, step 3).
     * </p>
     *
     * @param round The round echoed in the response.
     * @param senderRaftId The RaftId of the responding follower.
     * @param commitIndex The leader's current commit index.
     */
    public void confirm(long round, String senderRaftId, long commitIndex) {
        for (Entry entry : requests.headMap(round, true).values()) {
            entry.accepted(senderRaftId);
        }
        advance(commitIndex);
    }

    /**
     * Registers new reads and returns the round the request sent for them must carry.
     *
     * <p>
     * Only responses echoing this round, or a later one, confirm the reads. If the repository was already destroyed by
     * invoking {@link #destroy()}, or the leader alone is a majority, the reads complete immediately and the returned
     * round is 0.
     * </p>
     *
     * @param commitIndex The commit index at the time the requests were submitted.
     * @param reads The requests to store.
     * @param leaderRaftId The RaftId of the current leader.
     * @return the round for the request sent after the registration.
     */
    public long register(long commitIndex, Collection<R> reads, String leaderRaftId) {
        if (destroyed) {
            destroyer.accept(reads);
            return 0;
        }

        // Handle a case where the majority is made by a single node.
        // The leader is enough to consider majority reached, so we complete the request inline.
        if (majority.get() <= 1) {
            listener.accept(reads);
            return 0;
        }

        long readIndex = Math.max(commitIndex, minimumIndex);
        requests.put(++round, new Entry(reads, readIndex, leaderRaftId));
        return round;
    }

    public static <R> Builder<R> builder(Supplier<Integer> majority) {
        return new Builder<>(majority);
    }

    private final class Entry {
        private final Collection<R> requests;
        private final long readIndex;
        private final Set<String> votes;

        public Entry(Collection<R> requests, long readIndex, String leaderRaftId) {
            // No need to copy the requests collections since this class is the sole owner now.
            this.requests = requests;
            this.votes = new HashSet<>();
            this.readIndex = readIndex;
            accepted(leaderRaftId);
        }

        public void accepted(String senderRaftId) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Registering vote from %s, total %s for %s", senderRaftId, votes, requests);
            }
            votes.add(senderRaftId);
        }

        public boolean isConfirmed() {
            return votes.size() >= majority.get();
        }
    }

    public static final class Builder<R> {
        private final Supplier<Integer> majority;
        private Consumer<Collection<R>> listener = ignore -> {};
        private Consumer<Collection<R>> destroyer = ignore -> {};
        private long minimumIndex;

        private Builder(Supplier<Integer> majority) {
            this.majority = majority;
        }

        public Builder<R> withCommitter(Consumer<Collection<R>> committer) {
            this.listener = committer;
            return this;
        }

        public Builder<R> withDestroyer(Consumer<Collection<R>> destroyer) {
            this.destroyer = destroyer;
            return this;
        }

        /**
         * Sets the lowest index a request is registered at.
         *
         * <p>
         * A request must see every entry committed before it was submitted. A new leader may hold committed entries
         * without knowing they are committed, and learns it only by committing an entry of its own term (Raft paper,
         * §8). Passing the leader's last log index at election delays requests until those entries are committed.
         * </p>
         *
         * @param minimumIndex the last log index when the member became leader.
         * @return this builder.
         */
        public Builder<R> withMinimumIndex(long minimumIndex) {
            this.minimumIndex = minimumIndex;
            return this;
        }

        public ReadOnlyRequestRepository<R> build() {
            return new ReadOnlyRequestRepository<>(minimumIndex, majority, listener, destroyer);
        }
    }
}
