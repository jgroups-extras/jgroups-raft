package org.jgroups.protocols.raft;

import org.jgroups.Address;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory log implementation without any persistence. Used by some unit tests
 * @author Bela Ban
 * @since  0.2
 */
public class InMemoryLog implements Log {
    protected String       name; // the name of this log
    protected int          current_term;
    protected Address      voted_for;
    protected int          first_applied;
    protected int          last_applied;
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
            first_applied=existing.first_applied;
            last_applied=existing.last_applied;
            commit_index=existing.commit_index;
            entries=existing.entries;
        }
        else {
            current_term=0;
            voted_for=null;
            first_applied=0;
            last_applied=0;
            commit_index=0;
            entries=new LogEntry[16];
        }
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
        //if(new_index > last_applied)
          //  throw new IllegalStateException("commit_index (" + commit_index + ") cannot be set to " + new_index +
            //                                  " as last_applied is " + last_applied);
        commit_index=new_index; return this;
    }

    @Override
    public int firstApplied() {return first_applied;}

    @Override
    public int lastApplied() {return last_applied;}

    @Override
    public synchronized void append(int index, boolean overwrite, LogEntry... new_entries) {
        int space_required=new_entries != null? new_entries.length : 0;
        int available_space=this.entries.length - last_applied;
        if(space_required > available_space)
            expand(space_required - available_space +1);

        for(LogEntry entry: new_entries) {
            int idx=index-first_applied;
            if(!overwrite && this.entries[idx] != null)
                throw new IllegalStateException("Index " + index + " already contains a log entry: " + this.entries[idx]);
            this.entries[idx]=entry;
            last_applied=Math.max(last_applied, index);
            index++;
            if(entry.term > current_term)
                current_term=entry.term;
        }
    }

    @Override
    public AppendResult append(int prev_index, int prev_term, LogEntry... entries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized LogEntry get(int index) {
        int real_index=index - first_applied;
        return real_index < 0 || real_index >= entries.length? null : entries[real_index];
    }

    @Override
    public synchronized void truncate(int index) {
        if(index > commit_index)
            index=commit_index;
        LogEntry[] tmp=new LogEntry[entries.length];
        int idx=index-first_applied;
        System.arraycopy(entries, idx, tmp, 0, entries.length - idx);
        entries=tmp;
        first_applied=index;
    }

    @Override
    public synchronized void deleteAllEntriesStartingFrom(int start_index) {
        int idx=start_index-first_applied;
        if(idx < 0 || idx >= entries.length)
            return;

        for(int index=idx; index <= last_applied; index++)
            entries[index]=null;

        LogEntry last=get(start_index - 1);
        current_term=last != null? last.term : 0;
        last_applied=start_index-1;
        if(commit_index > last_applied)
            commit_index=last_applied;
    }

    @Override
    public synchronized void forEach(Function function, int start_index, int end_index) {
        if(start_index < first_applied)
            start_index=first_applied;
        if(end_index > last_applied)
            end_index=last_applied;

        int start=Math.max(1, start_index)-first_applied, end=end_index-first_applied;
        for(int i=start; i <= end; i++) {
            LogEntry entry=entries[i];
            if(!function.apply(start_index, entry.term, entry.command, entry.offset, entry.length))
                break;
            start_index++;
        }
    }

    @Override
    public synchronized void forEach(Function function) {
        forEach(function, Math.max(1, first_applied), last_applied);
    }


    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("first_applied=").append(first_applied).append(", last_applied=").append(last_applied)
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
