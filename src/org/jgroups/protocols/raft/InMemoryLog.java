package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.raft.util.ArrayRingBuffer;
import org.jgroups.raft.util.RWReference;

import net.jcip.annotations.ThreadSafe;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

/**
 * An in-memory {@link Log} implementation without any persistence.
 * <p>
 * This implementation holds the log metadata behind a read-write lock. The actual {@link LogEntry} are stored
 * with a {@link ArrayRingBuffer}, resizing as necessary. The entries are only freed from memory on a
 * {@link Log#truncate(long)} operation, so it needs a proper configuration to avoid OOM.
 * <p>
 * <b>Warning:</b> This implementation does <b>not</b> tolerate restarts, meaning all internal states
 * <b>will be lost</b>. If data must survive restarts, use another implementation.
 *
 * @author Bela Ban
 * @since  0.2
 * @see Log
 */
@ThreadSafe
public class InMemoryLog implements Log {
    // keeps all logs, keyed by name
    public static final Map<String,Log> logs=new ConcurrentHashMap<>();
    private final RWReference<LogMetadata> metadata = new RWReference<>(new LogMetadata());

    protected String                name; // the name of this log
    protected volatile Address      voted_for;
    protected volatile ByteBuffer   snapshot;

    public InMemoryLog() { }

    @Override
    public void init(String log_name, Map<String,String> args) throws Exception {
        name=log_name;
        InMemoryLog existing=(InMemoryLog)logs.putIfAbsent(name, this);
        if(existing != null) {
            voted_for=existing.voted_for;
            metadata.write(curr -> {
                existing.metadata.read(curr::copy);
            });
        }
        else {
            voted_for=null;
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
            l.metadata.write(LogMetadata::delete);
        }
    }

    @Override
    public long currentTerm() {return metadata.read(m -> m.current_term);}

    @Override
    public Log currentTerm(long new_term) {
        metadata.write(m -> {m.current_term=new_term;});
        return this;
    }

    @Override
    public Address votedFor() {return voted_for;}

    @Override
    public Log votedFor(Address member) {
        voted_for=member; return this;
    }

    @Override
    public long commitIndex() {return metadata.read(m -> m.commit_index);}

    @Override
    public Log commitIndex(long new_index) {
        metadata.write(m -> { m.commit_index=new_index; });
        return this;
    }

    @Override
    public long firstAppended() {return metadata.read(m -> m.first_appended);}

    @Override
    public long lastAppended() {return metadata.read(m -> m.last_appended);}


    public void setSnapshot(ByteBuffer sn) {
        this.snapshot=sn;
    }

    public ByteBuffer getSnapshot() {
        return snapshot != null? snapshot.duplicate() : null;
    }

    @Override
    public long append(long index, LogEntries entries) {
        LogEntry[] elements = entries.entries.toArray(new LogEntry[entries.size()]);
        long term = 0;
        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.entries.get(i);
            if (entry.term > term) term = entry.term;
        }

        long candidate_term = term;
        long candidate_last = index + entries.size() - 1;
        return metadata.write(m -> {
            for (LogEntry entry : elements) m.entries.add(entry);
            m.last_appended=Math.max(m.last_appended, candidate_last);
            m.current_term=Math.max(m.current_term, candidate_term);
            return m.last_appended;
        });
    }


    @Override
    public LogEntry get(long index) {
        if (index <= 0) return null;

        return metadata.read(m -> {
            int real_index=(int)(index - m.first_appended);
            return real_index < 0 || m.entries.isEmpty() || real_index > m.entries.size() ? null : m.entries.get(index);
        });
    }

    @Override
    public void truncate(long index_exclusive) {
        metadata.write(m -> {
            long actual_index = Math.min(index_exclusive, m.commit_index);
            m.entries.dropHeadUntil(actual_index);
            m.first_appended=actual_index;
            if (m.last_appended < actual_index) {
                m.last_appended =actual_index;
            }
        });
    }

    @Override
    public void reinitializeTo(long index, LogEntry entry) {
        metadata.write(m -> {
            m.current_term=entry.term();
            m.entries.dropHeadUntil(index);
            m.entries.dropTailToHead();
            m.first_appended=m.commit_index=m.last_appended=index;
        });
    }

    @Override
    public void deleteAllEntriesStartingFrom(long start_index) {
        metadata.write(m -> {
            int idx=(int)(start_index- m.first_appended);
            if(idx < 0 || idx > m.entries.size())
                return;

            assert start_index > m.commit_index; // the commit index cannot go back!

            m.entries.dropTailTo(start_index);
            LogEntry last=get(start_index - 1);
            m.current_term=last != null? last.term : 0;
            m.last_appended=start_index-1;
            if(m.commit_index > m.last_appended)
                m.commit_index=m.last_appended;
        });
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) {
        metadata.read(m -> {
            long start = Math.max(start_index, m.entries.getHeadSequence());
            long end = Math.min(end_index, m.entries.getTailSequence());

            for(long i=start; i < end; i++) {
                LogEntry entry=m.entries.get(i);
                function.accept(entry, i);
            }
        });
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function) {
        metadata.read(m -> {
            forEach(function, Math.max(1, m.entries.getHeadSequence()), m.entries.getTailSequence());
        });
    }

    public long sizeInBytes() {
        return metadata.read(m -> {
            AtomicLong size = new AtomicLong(0);
            m.entries.forEach((entry, ignore) -> size.addAndGet(entry.length));
            return size.longValue();
        });
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        return metadata.read(m -> {
            sb.append("first_appended=").append(m.first_appended).append(", last_appended=").append(m.last_appended)
                    .append(", commit_index=").append(m.commit_index).append(", current_term=").append(m.current_term);
            return sb.toString();
        });
    }

    private static class LogMetadata {
        private long                      current_term;
        private long                      first_appended;
        private long                      last_appended;
        private long                      commit_index;
        private ArrayRingBuffer<LogEntry> entries;

        public LogMetadata() {
            this.entries = new ArrayRingBuffer<>(16, 1);
            this.current_term = 0;
            this.first_appended = 0;
            this.last_appended = 0;
            this.commit_index = 0;
        }

        public void copy(LogMetadata other) {
            this.current_term = other.current_term;
            this.first_appended = other.first_appended;
            this.last_appended = other.last_appended;
            this.commit_index = other.commit_index;
            this.entries = other.entries;
        }

        public void delete() {
            this.current_term = 0;
            this.first_appended = 0;
            this.last_appended = 0;
            this.commit_index = 0;
        }
    }
}
