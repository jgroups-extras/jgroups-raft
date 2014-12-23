package org.jgroups.util;


import org.jgroups.Address;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Keeps track of AppendRequest messages and responses. Each AppendEntry request is keyed by the index at which
 * it was inserted at the leader. The values (RequestEntry) contain the responses from followers. When a response
 * is added, and the majority has been reached, add() retuns true and the key/value pair will be removed.
 * (subsequent responses will be ignored). On a majority, the commitIndex is advanced.
 * <p/>
 * Only created on leader
 * @author Bela Ban
 * @since 0.1
 */
public class RequestTable {
    protected static class Entry {
        // the future has been returned to the caller, and needs to be notified when we've reached a majority
        protected final CompletableFuture<byte[]> client_future;
        protected final Set<Address> votes=new HashSet<>(); // todo: replace with bitset
        protected boolean                         committed;

        public Entry(CompletableFuture<byte[]> client_future, Address vote) {
            this.client_future=client_future;
            votes.add(vote);
        }

        protected boolean add(Address vote, int majority) {
            boolean reached_majority=votes.add(vote) && votes.size() >= majority;
            return reached_majority && !committed && (committed=true);
        }

        @Override
        public String toString() {
            return "committed=" + committed + ", votes=" + votes;
        }
    }

    // protected final View view; // majority computed as view.size()/2+1
    protected final int                majority;

    // maps an index to a set of (response) senders
    protected final Map<Integer,Entry> requests=new HashMap<>();

    public RequestTable(int majority) {
        this.majority=majority;
    }

    /** Whether or not the entry at index is committed */
    public synchronized boolean isCommitted(int index) {
        Entry entry=requests.get(index);
        return entry != null && entry.committed;
    }

    public synchronized void create(int index, Address vote, CompletableFuture<byte[]> future) {
        Entry entry=new Entry(future, vote);
        requests.put(index, entry);
    }

    /**
     * Adds a response to the response set. If the majority has been reached, returns true
     * @return True if a majority has been reached, false otherwise. Note that this is done <em>exactly once</em>
     */
    public synchronized boolean add(int index, Address sender) {
        Entry entry=requests.get(index);
        return entry != null && entry.add(sender, majority);
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
            entry.client_future.complete(value);
            requests.remove(index);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Integer,Entry> entry: requests.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        return sb.toString();
    }
}
