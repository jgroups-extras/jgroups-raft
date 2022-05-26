package org.jgroups.protocols.raft;

import org.jgroups.Address;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ObjLongConsumer;

/**
 * In-memory log implementation without any persistence. Used only by unit tests. Note that synchronization is
 * half-baked.
 * @author Bela Ban
 * @since  0.2
 */
public class InMemoryLog implements Log {
    protected String       name; // the name of this log
    protected long         current_term;
    protected Address      voted_for;
    protected long         first_appended;
    protected long         last_appended;
    protected long         commit_index;
    protected LogEntry[]   entries;
    protected ByteBuffer   snapshot;

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
    public long currentTerm() {return current_term;}

    @Override
    public synchronized Log currentTerm(long new_term) {
        current_term=new_term; return this;
    }

    @Override
    public Address votedFor() {return voted_for;}

    @Override
    public synchronized Log votedFor(Address member) {
        voted_for=member; return this;
    }

    @Override
    public long commitIndex() {return commit_index;}

    @Override
    public synchronized Log commitIndex(long new_index) {
        commit_index=new_index; return this;
    }

    @Override
    public long firstAppended() {return first_appended;}

    @Override
    public long lastAppended() {return last_appended;}


    public void setSnapshot(ByteBuffer sn) {
        this.snapshot=sn;
    }

    public ByteBuffer getSnapshot() {
        return snapshot != null? snapshot.duplicate() : null;
    }

    @Override
    public synchronized long append(long index, LogEntries entries) {
        int space_required=entries != null? entries.size() : 0;
        long available_space=this.entries.length - 1 - last_appended;
        if(space_required > available_space)
            expand((int)(space_required - available_space +1));

        for(LogEntry entry: entries) {
            int idx=(int)(index-first_appended); // we cannot be more than 2^31 (max size of int) apart!
            this.entries[idx]=entry;
            last_appended=Math.max(last_appended, index);
            index++;
            if(entry.term > current_term)
                current_term=entry.term;
        }
        return last_appended;
    }


    @Override
    public synchronized LogEntry get(long index) {
        int real_index=(int)(index - first_appended);
        return real_index < 0 || real_index >= entries.length? null : entries[real_index];
    }

    @Override
    public synchronized void truncate(long index_exclusive) {
        if(index_exclusive > commit_index)
            index_exclusive=commit_index;
        LogEntry[] tmp=new LogEntry[entries.length];
        int idx=(int)(index_exclusive - first_appended);
        System.arraycopy(entries, idx, tmp, 0, entries.length - idx);
        entries=tmp;
        first_appended=index_exclusive;
        if (last_appended < index_exclusive) {
            last_appended =index_exclusive;
        }
    }

    @Override
    public void reinitializeTo(long index, LogEntry entry) {
        Arrays.fill(entries, null);
        first_appended=commit_index=last_appended=index;
        current_term=entry.term();
    }

    @Override
    public synchronized void deleteAllEntriesStartingFrom(long start_index) {
        int idx=(int)(start_index- first_appended);
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
    public synchronized void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) {
        if(start_index < first_appended)
            start_index=first_appended;
        if(end_index > last_appended)
            end_index=last_appended;

        int start=(int)(Math.max(1, start_index)- first_appended), end=(int)(end_index- first_appended);
        for(int i=start; i <= end; i++) {
            LogEntry entry=entries[i];
            function.accept(entry, i);
        }
    }

    @Override
    public synchronized void forEach(ObjLongConsumer<LogEntry> function) {
        forEach(function, Math.max(1, first_appended), last_appended);
    }

    public long sizeInBytes() {
        long size=0;
        int start_index=(int)Math.max(first_appended, 1);
        for(int i=start_index; i <= last_appended; i++) {
            LogEntry entry=entries[i];
            if(entry != null)
                size+=entry.length;
        }
        return size;
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
