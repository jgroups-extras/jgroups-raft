package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.raft.util.ArrayRingBuffer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

/**
 * An in-memory {@link Log} implementation without any persistence.
 * <p>
 * The actual {@link LogEntry} are stored with a {@link ArrayRingBuffer}, resizing as necessary. The entries
 * are only freed from memory on a {@link Log#truncate(long)} operation, so it needs a proper
 * configuration to avoid OOM.
 * <p>
 * <b>Warning:</b> This implementation does <b>not</b> tolerate restarts, meaning all internal states
 * <b>will be lost</b>. If data must survive restarts, use another implementation.
 *
 * @author Bela Ban
 * @since  0.2
 * @see Log
 */
public class InMemoryLog implements Log {
    // keeps all logs, keyed by name
    public static final Map<String,Log> logs=new ConcurrentHashMap<>();
    private long                      current_term;
    private long                      first_appended;
    private long                      last_appended;
    private long                      commit_index;
    private ArrayRingBuffer<LogEntry> entries;
    protected String                  name; // the name of this log
    protected volatile Address        voted_for;
    protected volatile ByteBuffer     snapshot;

    public InMemoryLog() { }

    @Override
    public void init(String log_name, Map<String,String> args) throws Exception {
        name=log_name;
        InMemoryLog existing=(InMemoryLog)logs.putIfAbsent(name, this);
        if(existing != null) {
            voted_for=existing.voted_for;
            this.current_term = existing.current_term;
            this.first_appended = existing.first_appended;
            this.last_appended = existing.last_appended;
            this.commit_index = existing.commit_index;
            this.entries = existing.entries;
        }
        else {
            voted_for=null;
            this.entries = new ArrayRingBuffer<>(16, 1);
            this.current_term = 0;
            this.first_appended = 0;
            this.last_appended = 0;
            this.commit_index = 0;
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
        InMemoryLog l = (InMemoryLog) logs.remove(name);
        if (l != null) {
            l.current_term = 0;
            l.first_appended = 0;
            l.last_appended = 0;
            l.commit_index = 0;
        }
    }

    @Override
    public long currentTerm() {return current_term;}

    @Override
    public Log currentTerm(long new_term) {
        current_term=new_term;
        return this;
    }

    @Override
    public Address votedFor() {return voted_for;}

    @Override
    public Log votedFor(Address member) {
        voted_for=member; return this;
    }

    @Override
    public long commitIndex() {return commit_index;}

    @Override
    public Log commitIndex(long new_index) {
        commit_index=new_index;
        return this;
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
    public long append(long index, LogEntries entries) {
        long candidate_term = entries.entries.get(entries.size() - 1).term;
        long candidate_last = index + entries.size() - 1;
        for (LogEntry entry : entries) {
            this.entries.add(entry);
        }
        last_appended=Math.max(last_appended, candidate_last);
        current_term=Math.max(current_term, candidate_term);
        return last_appended;
    }


    @Override
    public LogEntry get(long index) {
        if (index <= 0) return null;

        int real_index=(int)(index - first_appended);
        return real_index < 0 || entries.isEmpty() || real_index > entries.size()
                ? null
                : entries.get(index);
    }

    @Override
    public void truncate(long index_exclusive) {
        long actual_index = Math.min(index_exclusive, commit_index);
        entries.dropHeadUntil(actual_index);
        first_appended=actual_index;
        if (last_appended < actual_index) {
            last_appended =actual_index;
        }
    }

    @Override
    public void reinitializeTo(long index, LogEntry entry) {
        current_term=entry.term();
        entries.dropHeadUntil(index);
        entries.dropTailToHead();
        first_appended=commit_index=last_appended=index;
    }

    @Override
    public void deleteAllEntriesStartingFrom(long start_index) {
        int idx=(int)(start_index- first_appended);
        if(idx < 0 || idx > entries.size())
            return;

        assert start_index > commit_index; // the commit index cannot go back!

        entries.dropTailTo(start_index);
        LogEntry last=get(start_index - 1);
        current_term=last != null? last.term : 0;
        last_appended=start_index-1;
        if(commit_index > last_appended)
            commit_index=last_appended;
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) {
        long start = Math.max(start_index, entries.getHeadSequence());
        long end = Math.min(end_index, entries.getTailSequence());

        for(long i=start; i < end; i++) {
            LogEntry entry=entries.get(i);
            function.accept(entry, i);
        }
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function) {
        forEach(function, Math.max(1, entries.getHeadSequence()), entries.getTailSequence());
    }

    public long sizeInBytes() {
        AtomicLong size = new AtomicLong(0);
        entries.forEach((entry, ignore) -> size.addAndGet(entry.length));
        return size.longValue();
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("first_appended=").append(first_appended).append(", last_appended=").append(last_appended)
                .append(", commit_index=").append(commit_index).append(", current_term=").append(current_term);
        return sb.toString();
    }
}
