package org.jgroups.raft.util;


import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

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
    protected final NavigableMap<Integer,Entry<T>> requests=new ConcurrentSkipListMap<>();


    public void create(int index, T vote, CompletableFuture<byte[]> future, int majority) {
        Entry<T> entry=new Entry<>(future);
        synchronized(this) {
            requests.put(index, entry);
            entry.add(vote, majority);
        }
    }

    /**
     * Adds a response to the response set. If the majority has been reached, returns true
     * @return True if a majority has been reached, false otherwise. Note that this is done <em>exactly once</em>
     */
    public synchronized boolean add(int index, T sender, int majority) {
        // we're getting an ack for index, but we also need to ack entries lower than index (if any, should only
        // happen on leader change): https://github.com/belaban/jgroups-raft/issues/122

        if(requests.lowerKey(index) != null) {
            // we have entries with indices lower than index; ack them, too
            Map<Integer,Entry<T>> lower_entries=requests.headMap(index);
            lower_entries.values().stream().filter(Objects::nonNull).forEach(e -> e.add(sender, majority));
        }
        Entry<T> entry=requests.get(index);
        return entry != null && entry.add(sender, majority);
    }

    /** Whether or not the entry at index is committed */
    public synchronized boolean isCommitted(int index) {
        Entry<?> entry=requests.get(index);
        return entry != null && entry.committed;
    }

    /** number of requests being processed */
    public synchronized int size() {
        return requests.size();
    }

    /** Notifies the CompletableFuture and then removes the entry for index */
    public synchronized void notifyAndRemove(int index, byte[] response) {
        Entry<?> entry=requests.get(index);
        if(entry != null) {
            if(entry.client_future != null)
                entry.client_future.complete(response);
            requests.remove(index);
        }
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Integer,Entry<T>> entry: requests.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
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
            votes.add(vote);
            return votes.size() >= majority && commit();
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
