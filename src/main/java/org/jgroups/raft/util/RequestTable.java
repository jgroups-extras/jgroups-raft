package org.jgroups.raft.util;


import org.jgroups.raft.Options;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Keeps track of {@link org.jgroups.protocols.raft.AppendEntriesRequest} messages and responses.
 * <p>
 * Each AppendEntry request is keyed by the index at which
 * it was inserted at the leader. The values (RequestEntry) contain the responses from followers. When a response
 * is added, and the majority has been reached, add() returns true and the key/value pair will be removed.
 * (subsequent responses will be ignored). On a majority, the commit index is advanced.
 * </p>
 * Only created on leader
 * @author Bela Ban
 * @since 0.1
 */
public class RequestTable<T> {
    // maps an index to a set of (response) senders
    protected ArrayRingBuffer<Entry<T>> requests;

    // Identify the request table was destroyed.
    // All subsequent requests should complete exceptionally immediately.
    private Throwable destroyed;


    public void create(long index, T vote, CompletableFuture<byte[]> future, Supplier<Integer> majority) {
        create(index, vote, future, majority, null);
    }

    public void create(long index, T vote, CompletableFuture<byte[]> future, Supplier<Integer> majority, Options opts) {
        Entry<T> entry=new Entry<>(future, opts);
        if (requests == null) {
            requests = new ArrayRingBuffer<>(index);
        }
        requests.set(index, entry);
        entry.add(vote, majority);
        // In case the leader steps down while still adding elements.
        if (destroyed != null) entry.notify(destroyed);
    }

    /**
     * Completes all uncommitted requests with the provided exception.
     *
     * <p>
     * This method should be invoked before setting the instance to <code>null</code> when the leader steps down.
     * This provides a more responsive completion of requests to the users, instead of having requests time out.
     * Internal operations, such as membership changes, do not have a timeout associated, which would hang and any
     * subsequent changes would not complete.
     * </p>
     *
     * @param t: Throwable to complete the requests exceptionally.
     */
    public void destroy(Throwable t) {
        // Keep throwable so any entries created *after* destroying also complete exceptionally.
        destroyed = t;
        if (requests != null) {
            requests.forEach((e, ignore) -> {
                if (!e.committed) {
                    e.notify(t);
                }
            });
        }
    }

    /**
     * Adds a response to the response set. If the majority has been reached, returns true
     * @return True if a majority has been reached, false otherwise. Note that this is done <em>exactly once</em>
     */
    public boolean add(long index, T sender, Supplier<Integer> majority) {
        // we're getting an ack for index, but we also need to ack entries lower than index (if any, should only
        // happen on leader change): https://github.com/belaban/jgroups-raft/issues/122
        if (requests == null) {
            return false;
        }
        boolean added = false;
        for (long i = requests.getHeadSequence(); i <= Math.min(index, requests.getTailSequence() - 1); i++) {
            final Entry<T> entry = requests.get(i);
            if (entry == null) {
                continue;
            }
            final boolean entryAdded = entry.add(sender, majority);
            if (i == index && entryAdded) {
                added = true;
            }
        }
        return added;
    }

    /** Whether or not the entry at index is committed */
    public boolean isCommitted(long index) {
        if (requests == null) {
            return false;
        }
        if (index < requests.getHeadSequence()) {
            return true;
        }

        Entry<?> entry=requests.contains(index)? requests.get(index) : null;
        return entry != null && entry.committed;
    }

    /** number of requests being processed */
    public int size() {
        if(requests == null)
            return 0;
        return requests.size(false);
    }

    public Entry<T> remove(long index) {
        return requests != null? requests.remove(index) : null;
    }

    /** Notifies the CompletableFuture and then removes the entry for index */
    public void notifyAndRemove(long index, byte[] response) {
        Entry<?> entry=remove(index);
        if(entry != null && entry.client_future != null)
            entry.client_future.complete(response);
    }

    public String toString() {
        if (requests == null || requests.isEmpty()) {
            return "";
        }
        StringBuilder sb=new StringBuilder();
        requests.forEach((entry, index) -> sb.append(index).append(": ").append(entry).append("\n"));
        return sb.toString();
    }


    public static class Entry<T> {
        // the future has been returned to the caller, and needs to be notified when we've reached a majority
        protected final CompletableFuture<byte[]> client_future;
        protected final Set<T>                    votes=new HashSet<>();
        protected final Options                   opts;
        protected boolean                         committed;


        public Entry(CompletableFuture<byte[]> client_future, Options opts) {
            this.client_future=client_future;
            this.opts=opts;
        }

        public Options options() {return opts;}

        public boolean add(T vote, final Supplier<Integer> majority) {
            return votes.add(vote) && votes.size() >= majority.get() && commit();
        }

        public void notify(byte[] result) {
            if(client_future != null)
                client_future.complete(result);
        }

        public void notify(Throwable t) {
            if(client_future != null)
                client_future.completeExceptionally(t);
        }

        // returns true only the first time the entry is committed
        public boolean commit() {
            boolean prev_committed = committed;
            committed=true;
            return !prev_committed;
        }

        @Override
        public String toString() {
            return String.format("committed=%b, votes=%s %s", committed, votes, opts != null? opts.toString() : "");
        }
    }
}
