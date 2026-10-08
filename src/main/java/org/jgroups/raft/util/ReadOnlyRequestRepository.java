package org.jgroups.raft.util;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Repository for read-only operations.
 *
 * <p>
 * This repository utilizes an ordered set to map between a log index and multiple pending requests. Read-only operations
 * in {@link org.jgroups.protocols.raft.RAFT} do not update the log index, in contrast with {@link RequestTable}. This
 * maps multiple requests to the same index in the log, or the same key in the requests map.
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
    private boolean destroyed;

    private ReadOnlyRequestRepository(Supplier<Integer> majority, Consumer<Collection<R>> listener, Consumer<Collection<R>> destroyer) {
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
     * Advances the commit index.
     * <p>
     * Once the commit index advances, this method will complete the requests up-to, and including, the index. The listener
     * is invoked for all requests in the range.
     * </p>
     *
     * @param index The current index the log was advanced to.
     */
    public void advance(long index) {
        Iterator<Map.Entry<Long, Entry>> it = requests.headMap(index, true).entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Entry> me = it.next();
            Entry v = me.getValue();
            if (v.isCommitted() || me.getKey() < index) {
                listener.accept(v.requests);
                it.remove();
            }
        }
    }

    /**
     * Register an accept from a follower to the given index.
     * <p>
     * Register the accept response of a single follower node to all requests of a given index and below. The completion
     * listener is invoked once a majority of accepts is registered.
     * </p>
     *
     * @param index The index accepted by the follower node.
     * @param senderRaftId The RaftId of the node which is committing the entry.
     */
    public void commit(long index, String senderRaftId) {
        Iterator<Map.Entry<Long, Entry>> it = requests.headMap(index, true).entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Entry> me = it.next();
            Entry v = me.getValue();
            v.accepted(senderRaftId);
            if (v.isCommitted()) {
                listener.accept(v.requests);
                it.remove();
            }
        }
    }

    /**
     * Register a new request submitted at the given index.
     * <p>
     * If the repository was already destroyed by invoking {@link #destroy()}, the destroyer listener is invoked with
     * the provided request.
     * </p>
     *
     * @param index The commit index at the time the request was submitted.
     * @param request The request to store.
     * @param leaderRaftId The RaftId of the current leader.
     */
    public void register(long index, R request, String leaderRaftId) {
        if (destroyed) {
            destroyer.accept(Collections.singletonList(request));
            return;
        }

        requests.compute(index, (k, v) -> {
            if (v == null) return new Entry(request, leaderRaftId);

            v.include(request);
            return v;
        });
    }

    public static <R> Builder<R> builder(Supplier<Integer> majority) {
        return new Builder<>(majority);
    }

    private final class Entry {
        private final Collection<R> requests;
        private final Set<String> votes;

        public Entry(R request, String leaderRaftId) {
            this.requests = new ArrayList<>();
            this.votes = new HashSet<>();
            requests.add(request);
            accepted(leaderRaftId);
        }

        public void include(R request) {
            requests.add(request);
        }

        public void accepted(String senderRaftId) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Registering vote from %s, total %s for %s", senderRaftId, votes, requests);
            }
            votes.add(senderRaftId);
        }

        public boolean isCommitted() {
            return votes.size() >= majority.get();
        }
    }

    public static final class Builder<R> {
        private final Supplier<Integer> majority;
        private Consumer<Collection<R>> listener = ignore -> {};
        private Consumer<Collection<R>> destroyer = ignore -> {};

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

        public ReadOnlyRequestRepository<R> build() {
            return new ReadOnlyRequestRepository<>(majority, listener, destroyer);
        }
    }
}
