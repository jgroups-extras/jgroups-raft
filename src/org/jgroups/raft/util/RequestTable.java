package org.jgroups.raft.util;


import java.util.*;
import java.util.concurrent.CompletableFuture;

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


    public void create(int index, T vote, CompletableFuture<byte[]> future, int majority) {
        Entry<T> entry=new Entry<>(future);
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
    public boolean add(int index, T sender, int majority) {
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
        if (requests == null) {
            return 0;
        }
        return requests.size();
    }

    /** Notifies the CompletableFuture and then removes the entry for index */
    public void notifyAndRemove(int index, byte[] response) {
        if (requests == null) {
            return;
        }
        final Entry<?> entry = requests.remove(index);
        if (entry != null) {
            if (entry.client_future != null)
                entry.client_future.complete(response);
        }
    }

    public String toString() {
        if (requests == null || requests.isEmpty()) {
            return "";
        }
        StringBuilder sb=new StringBuilder();
        requests.forEach((entry, index) -> sb.append(index).append(": ").append(entry).append("\n"));
        return sb.toString();
    }


    protected static class Entry<T> {
        // the future has been returned to the caller, and needs to be notified when we've reached a majority
        protected final CompletableFuture<byte[]> client_future;
        protected final Set<T>                    votes=new HashSet<>();
        protected boolean                         committed;

        public Entry(CompletableFuture<byte[]> client_future) {
            this.client_future=client_future;
        }

        protected boolean add(T vote, int majority) {
            return votes.add(vote) && votes.size() >= majority && commit();
        }

        // returns true only the first time the entry is committed
        protected boolean commit() {
            boolean prev_committed = committed;
            committed=true;
            return !prev_committed;
        }

        @Override
        public String toString() {
            return "committed=" + committed + ", votes=" + votes;
        }
    }
}
