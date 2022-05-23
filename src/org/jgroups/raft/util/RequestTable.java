package org.jgroups.raft.util;


import org.jgroups.raft.Options;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Keeps track of AppendRequest messages and responses. Each AppendEntry request is keyed by the index at which
 * it was inserted at the leader. The values (RequestEntry) contain the responses from followers. When a response
 * is added, and the majority has been reached, add() returns true and the key/value pair will be removed.
 * (subsequent responses will be ignored). On a majority, the commit index is advanced.
 * <p/>
 * Only created on leader
 * @author Bela Ban
 * @since 0.1
 */
public class RequestTable<T> {
    // maps an index to a set of (response) senders
    protected ArrayRingBuffer<Entry<T>> requests;


    public void create(int index, T vote, CompletableFuture<byte[]> future, Supplier<Integer> majority) {
        create(index, vote, future, majority, null);
    }

    public void create(int index, T vote, CompletableFuture<byte[]> future, Supplier<Integer> majority, Options opts) {
        Entry<T> entry=new Entry<>(future, opts);
        if (requests == null) {
            requests = new ArrayRingBuffer<>(index);
        }
        requests.set(index, entry);
        entry.add(vote, majority);
    }

    /**
     * Adds a response to the response set. If the majority has been reached, returns true
     * @return True if a majority has been reached, false otherwise. Note that this is done <em>exactly once</em>
     */
    public boolean add(int index, T sender, Supplier<Integer> majority) {
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
    public boolean isCommitted(int index) {
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

    public Entry<T> remove(int index) {
        return requests != null? requests.remove(index) : null;
    }

    /** Notifies the CompletableFuture and then removes the entry for index */
    public void notifyAndRemove(int index, byte[] response) {
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
