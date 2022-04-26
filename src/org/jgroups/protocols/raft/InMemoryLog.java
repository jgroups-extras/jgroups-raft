package org.jgroups.protocols.raft;

import org.jgroups.Address;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ObjIntConsumer;

/**
 * In-memory log implementation without any persistence. Used only by unit tests. Note that synchronization is
 * half-baked.
 * @author Bela Ban
 * @since  0.2
 */
public class InMemoryLog implements Log {
    protected String       name; // the name of this log
    protected int          current_term;
    protected Address      voted_for;
    protected int          first_appended;
    protected int          last_appended;
    protected int          commit_index;
    protected LogEntry[]   entries;

    // keeps all logs, keyed by name
    public static final Map<String,Log> logs=new ConcurrentHashMap<>();
    protected static final int          INCR=16; // always increase entries by 16 when needed


    public InMemoryLog() {
    }

    @Override
    public void init(String log_name, Map<String,String> args) throws Exception {
        name=log_name;
        InMemoryLog existing=(InMemoryLog)logs.putIfAbsent(name, this);
        if(existing != null) {
            current_term=existing.current_term;
            voted_for=existing.voted_for;
            first_appended=existing.first_appended;
            last_appended=existing.last_appended;
            commit_index=existing.commit_index;
            entries=existing.entries;
        }
        else {
            current_term=0;
            voted_for=null;
            first_appended=0;
            last_appended=0;
            commit_index=0;
            entries=new LogEntry[16];
        }
    }

    public Log useFsync(boolean f) {
        return this;
    }

    public boolean useFsync() {
        return false;
    }

    @Override
    public void close() {}

    @Override
    public void delete() {
        logs.remove(name);
    }

    @Override
    public int currentTerm() {return current_term;}

    @Override
    public synchronized Log currentTerm(int new_term) {
        current_term=new_term; return this;
    }

    @Override
    public Address votedFor() {return voted_for;}

    @Override
    public synchronized Log votedFor(Address member) {
        voted_for=member; return this;
    }

    @Override
    public int commitIndex() {return commit_index;}

    @Override
    public synchronized Log commitIndex(int new_index) {
        //if(new_index > last_appended)
          //  throw new IllegalStateException("commit_index (" + commit_index + ") cannot be set to " + new_index +
            //                                  " as last_appended is " + last_appended);
        commit_index=new_index; return this;
    }

    @Override
    public int firstAppended() {return first_appended;}

    @Override
    public int lastAppended() {return last_appended;}

    @Override
    public synchronized int append(int index, LogEntries entries) {
        int space_required=entries != null? entries.size() : 0;
        int available_space=this.entries.length - 1 - last_appended;
        if(space_required > available_space)
            expand(space_required - available_space +1);

        for(LogEntry entry: entries) {
            int idx=index-first_appended;
            this.entries[idx]=entry;
            last_appended=Math.max(last_appended, index);
            index++;
            if(entry.term > current_term)
                current_term=entry.term;
        }
        return last_appended;
    }


    @Override
    public synchronized LogEntry get(int index) {
        int real_index=index - first_appended;
        return real_index < 0 || real_index >= entries.length? null : entries[real_index];
    }

    @Override
    public synchronized void truncate(int index_exclusive) {
        if(index_exclusive > commit_index)
            index_exclusive=commit_index;
        LogEntry[] tmp=new LogEntry[entries.length];
        int idx=index_exclusive - first_appended;
        System.arraycopy(entries, idx, tmp, 0, entries.length - idx);
        entries=tmp;
        first_appended=index_exclusive;
        if (last_appended < index_exclusive) {
            last_appended =index_exclusive;
        }
    }

    @Override
    public void reinitializeTo(int index, LogEntry entry) {
        Arrays.fill(entries, null);
        first_appended=commit_index=last_appended=index;
        current_term=0;
    }

    @Override
    public synchronized void deleteAllEntriesStartingFrom(int start_index) {
        int idx=start_index- first_appended;
        if(idx < 0 || idx >= entries.length)
            return;

        assert start_index > commit_index; // the commit index cannot go back!

        for(int index=idx; index <= last_appended; index++)
            entries[index]=null;

        LogEntry last=get(start_index - 1);
        current_term=last != null? last.term : 0;
        last_appended=start_index-1;
        if(commit_index > last_appended)
            commit_index=last_appended;
    }

    @Override
    public synchronized void forEach(ObjIntConsumer<LogEntry> function, int start_index, int end_index) {
        if(start_index < first_appended)
            start_index=first_appended;
        if(end_index > last_appended)
            end_index=last_appended;

        int start=Math.max(1, start_index)- first_appended, end=end_index- first_appended;
        for(int i=start; i <= end; i++) {
            LogEntry entry=entries[i];
            function.accept(entry, i);
        }
    }

    @Override
    public synchronized void forEach(ObjIntConsumer<LogEntry> function) {
        forEach(function, Math.max(1, first_appended), last_appended);
    }


    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("first_appended=").append(first_appended).append(", last_appended=").append(last_appended)
          .append(", commit_index=").append(commit_index).append(", current_term=").append(current_term);
        return sb.toString();
    }

    /** Lock must be held to call this method */
    protected void expand(int min_size_needed) {
        LogEntry[] new_entries=new LogEntry[Math.max(entries.length + INCR, entries.length + min_size_needed)];
        System.arraycopy(entries, 0, new_entries, 0, entries.length);
        entries=new_entries;
    }
}
