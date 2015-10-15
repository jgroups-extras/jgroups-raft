package org.jgroups.raft.util;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    protected final Map<Integer,Entry<T>> requests=new HashMap<>(); // maps an index to a set of (response) senders


    public void create(int index, T vote, CompletableFuture<byte[]> future) {
        Entry<T> entry=new Entry<>(future, vote);
        synchronized(this) {
            requests.put(index, entry);
        }
    }

    /**
     * Adds a response to the response set. If the majority has been reached, returns true
     * @return True if a majority has been reached, false otherwise. Note that this is done <em>exactly once</em>
     */
    public synchronized boolean add(int index, T sender, int majority) {
        Entry<T> entry=requests.get(index);
        return entry != null && entry.add(sender, majority);
    }

    /** Whether or not the entry at index is committed */
    public synchronized boolean isCommitted(int index) {
        Entry entry=requests.get(index);
        return entry != null && entry.committed;
    }

    /** number of requests being processed */
    public synchronized int size() {
        return requests.size();
    }

    /** Notifies the CompletableFuture and then removes the entry for index */
    public synchronized void notifyAndRemove(int index, byte[] response, int offset, int length) {
        Entry entry=requests.get(index);
        if(entry != null) {
            byte[] value=response;
            if(response != null && offset > 0) {
                value=new byte[length];
                System.arraycopy(response, offset, value, 0, length);
            }
            if(entry.client_future != null)
                entry.client_future.complete(value);
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

        public Entry(CompletableFuture<byte[]> client_future, T vote) {
            this.client_future=client_future;
            votes.add(vote);
        }

        protected boolean add(T vote, int majority) {
            boolean reached_majority=votes.add(vote) && votes.size() >= majority;
            return reached_majority && !committed && (committed=true);
        }

        @Override
        public String toString() {
            return "committed=" + committed + ", votes=" + votes;
        }
    }
}
