package org.jgroups.util;

import org.jgroups.Address;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Keeps track of next_index and match_index for each cluster member (excluding this leader).
 * Used to (1) compute the commit_index and (2) to resend log entries to members which haven't yet seen them.<p/>
 * Only created on the leader
 * @author Bela Ban
 * @since  0.1
 */
public class CommitTable {

    public static interface Consumer<ADDR,ENTRY> {
        void apply(ADDR a, ENTRY e);
    }

    public static class Entry {
        protected int     next_index;
        protected int     match_index;
        protected int     commit_index;
        protected boolean snapshot_in_progress;

        public Entry(int next_index) {this.next_index=next_index;}

        public int nextIndex()              {return next_index;}
        public Entry nextIndex(int idx)     {next_index=idx; return this;}
        public int matchIndex()             {return match_index;}
        public Entry matchIndex(int idx)    {this.match_index=idx; return this;}
        public int commitIndex()            {return commit_index;}
        public Entry commitIndex(int idx)   {this.commit_index=idx; return this;}

        public boolean snapshotInProgress(boolean flag) {
            if(snapshot_in_progress == flag)
                return false;
            snapshot_in_progress=flag;
            return true;
        }

        @Override public String toString() {
            StringBuilder sb=new StringBuilder().append("match-index=").append(match_index);
            sb.append(", next-index=").append(next_index).append(", commit-index=").append(commit_index);
            if(snapshot_in_progress)
                sb.append(" (snapshot in progress)");
            return sb.toString();
        }
    }

    protected final ConcurrentMap<Address,Entry> map=new ConcurrentHashMap<>();

    public CommitTable(List<Address> members, int next_index) {
        adjust(members, next_index);
    }

    public void adjust(List<Address> members, int next_index) {
        map.keySet().retainAll(members);
        for(Address mbr: members) {
            if(!map.containsKey(mbr))
                map.putIfAbsent(mbr, new Entry(next_index));
        }
    }

    public CommitTable update(Address member, int match_index, int next_index, int commit_index) {
        Entry entry=map.get(member);
        if(entry == null)
            return this;
        entry.match_index=Math.max(match_index, entry.match_index);
        entry.next_index=next_index;
        entry.commit_index=Math.max(entry.commit_index, commit_index);
        return this;
    }

    public boolean snapshotInProgress(Address mbr, boolean flag) {
        Entry entry=map.get(mbr);
        return entry != null && entry.snapshotInProgress(flag);
    }


    /** Applies a function to all elements of the commit table */
    public void forEach(Consumer<Address,Entry> function) {
        for(Map.Entry<Address,Entry> entry: map.entrySet()) {
            Entry val=entry.getValue();
            function.apply(entry.getKey(), val);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Entry> entry: map.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        return sb.toString();
    }


}