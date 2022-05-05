package org.jgroups.raft.util;

import org.jgroups.Address;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Keeps track of next_index and match_index for each cluster member (excluding this leader).
 * Used to (1) compute the commit_index and (2) to resend log entries to members which haven't yet seen them.<p/>
 * Only created on the leader
 * @author Bela Ban
 * @since  0.1
 */
public class CommitTable {
    protected final ConcurrentMap<Address,Entry> map=new ConcurrentHashMap<>();


    public CommitTable(List<Address> members, int next_index) {
        adjust(members, next_index);
    }

    public Set<Address> keys()         {return map.keySet();}
    public Entry        get(Address a) {return map.get(a);}

    public void adjust(List<Address> members, int next_index) {
        map.keySet().retainAll(members);
        // entry is only created if mbr is not in map, reducing unneeded creations
        members.forEach(mbr -> map.computeIfAbsent(mbr, k -> new Entry(next_index)));
    }

    public CommitTable update(Address member, int match_index, int next_index, int commit_index, boolean single_resend) {
        return update(member, match_index, next_index, commit_index, single_resend, false);
    }

    public CommitTable update(Address member, int match_index, int next_index, int commit_index,
                              boolean single_resend, boolean overwrite) {
        Entry e=map.get(member);
        if(e == null)
            return this;
        e.match_index=overwrite? match_index : Math.max(match_index, e.match_index);
        e.next_index=Math.max(1, next_index);
        e.commit_index=Math.max(e.commit_index, commit_index);
        e.send_single_msg=single_resend;
        e.assertInvariant();
        return this;
    }


    /** Applies a function to all elements of the commit table */
    public void forEach(BiConsumer<Address,Entry> function) {
        for(Map.Entry<Address,Entry> entry: map.entrySet()) {
            Entry val=entry.getValue();
            function.accept(entry.getKey(), val);
        }
    }

    @Override
    public String toString() {
        return map.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
    }



    public static class Entry {
        protected int     commit_index; // the commit index of the given member

        protected int     match_index;  // the index of the highest entry known to be replicated to the member

        protected int     next_index;   // the next index to send; initialized to last_appended +1

        // set to true when next_index was decremented, so we only send a single entry on the next resend interval;
        // set to false when we receive an AppendEntries(true) response
        protected boolean send_single_msg;

        public Entry(int next_index) {this.next_index=next_index;}

        public int     commitIndex()                   {return commit_index;}
        public Entry   commitIndex(int idx)            {this.commit_index=idx; return this;}
        public int     matchIndex()                    {return match_index;}
        public Entry   matchIndex(int idx)             {this.match_index=idx; return this;}
        public int     nextIndex()                     {return next_index;}
        public Entry   nextIndex(int idx)              {next_index=idx; return this;}

        public boolean sendSingleMessage()             {return send_single_msg;}
        public Entry   sendSingleMessage(boolean flag) {this.send_single_msg=flag; return this;}


        public void assertInvariant() {
            assert commit_index <= match_index && match_index <= next_index : this;
        }

        @Override public String toString() {
            StringBuilder sb=new StringBuilder("commit-index=").append(commit_index)
              .append(", match-index=").append(match_index).append(", next-index=").append(next_index);
            if(send_single_msg)
                sb.append(" [send-single-msg]");
            return sb.toString();
        }
    }

}