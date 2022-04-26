package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.ObjIntConsumer;

/**
 * Bounded caching {@link org.jgroups.protocols.raft.Log} facade. Reads are returned from the cache (if available),
 * writes invalidate the corresponding entries. Keeps the last N entries only (N is configurable).
 * @author Bela Ban
 * @since  1.0.8
 */
public class LogCache implements Log {
    private static final int DEFAULT_MAX_SIZE = 1024;
    protected final Log                            log;
    protected ArrayRingBuffer<LogEntry>            cache;
    protected int                                  max_size;
    protected int                                  current_term, commit_index, first_appended, last_appended;
    protected Address                              voted_for;
    protected int                                  num_trims, num_hits, num_misses;


    public LogCache(Log log) {
        this(log, DEFAULT_MAX_SIZE);
    }

    public LogCache(Log log, int max_size) {
        this.log=Objects.requireNonNull(log);
        current_term=log.currentTerm();
        commit_index=log.commitIndex();
        first_appended=log.firstAppended();
        last_appended=log.lastAppended();
        voted_for=log.votedFor();
        this.max_size=max_size;
        cache=new ArrayRingBuffer<>(max_size, log.commitIndex());
    }

    public int     maxSize()           {return max_size;}
    public Log     maxSize(int s)      {max_size=s; return this;}
    public int     cacheSize()         {return cache.size();}
    public Log     log()               {return log;}
    public int     numTrims()          {return num_trims;}
    public int     numAccesses()       {return num_hits+num_misses;}
    public double  hitRatio()          {return numAccesses() == 0? 0 : (double)num_hits / numAccesses();}
    public Log     resetStats()        {num_trims=num_hits=num_misses=0; return this;}
    public Log     useFsync(boolean f) {log.useFsync(f); return this;}
    public boolean useFsync()          {return log.useFsync();}

    public void init(String log_name, Map<String,String> args) throws Exception {
        log.init(log_name, args);
    }

    public void delete() throws Exception {
        log.delete();
        current_term=commit_index=first_appended=last_appended=0;
        voted_for=null;
        cache.clear();
    }

    public int currentTerm() {
        return current_term;
    }

    public Log currentTerm(int new_term) {
        log.currentTerm(new_term);
        current_term=new_term; // if the above fails, current_term won't be set
        return this;
    }

    public Address votedFor() {
        return voted_for;
    }

    public Log votedFor(Address member) {
        log.votedFor(member);
        voted_for=member;
        return this;
    }

    public int commitIndex() {
        return commit_index;
    }

    public Log commitIndex(int new_index) {
        log.commitIndex(new_index);
        commit_index=new_index;
        cache.dropHeadUntil(new_index);
        return this;
    }

    public int firstAppended() {
        return first_appended;
    }

    public int lastAppended() {
        return last_appended;
    }

    @Override
    public int append(int index, LogEntries entries) {
        last_appended=log.append(index, entries);
        current_term=log.currentTerm();

        for(LogEntry le: entries) {
            final int logIndex = index++;
            if (logIndex >= cache.getHeadSequence()) {
                if (cache.availableCapacityWithoutResizing() == 0) {
                    // try trim here to see if we can save enlarging to happen
                    trim();
                }
                cache.set(logIndex, le);
            }
        }
        trim();
        return last_appended;
    }

    public LogEntry get(int index) {
        if(index > last_appended) // called by every append() to check if the entry is already present
            return null;
        if (index < cache.getHeadSequence()) {
            // this cannot be cached!
            num_misses++;
            assert !cache.contains(index);
            return log.get(index);
        }
        if (cache.contains(index)) {
            final LogEntry e = cache.get(index);
            if (e != null) {
                num_hits++;
                return e;
            }
        }
        final LogEntry e = log.get(index);
        if (e == null) {
            return null;
        }
        num_misses++;
        cache.set(index, e);
        trim();
        return e;
    }

    public void truncate(int index_exclusive) {
        log.truncate(index_exclusive);
        cache.dropHeadUntil(index_exclusive);
        // todo: first_appended should be set to the return value of truncate() (once it has been changed)
        first_appended=log.firstAppended();
        last_appended = log.lastAppended();
    }

    @Override
    public void reinitializeTo(int index, LogEntry le) {
        log.reinitializeTo(index, le);
        cache.clear();
        cache=new ArrayRingBuffer<>(max_size, index);
        cache.add(le);
        first_appended=log.firstAppended();
        commit_index=log.commitIndex();
        last_appended=log.lastAppended();
    }

    public void deleteAllEntriesStartingFrom(int start_index) {
        log.deleteAllEntriesStartingFrom(start_index);
        commit_index=log.commitIndex();
        last_appended=log.lastAppended();
        current_term=log.currentTerm();
        cache.dropTailTo(start_index);
    }

    public void forEach(ObjIntConsumer<LogEntry> function, int start_index, int end_index) {
        log.forEach(function, start_index, end_index); // don't cache; this function is not called frequently
    }

    public void forEach(ObjIntConsumer<LogEntry> function) {
        forEach(function, first_appended, last_appended);
    }

    public void close() throws IOException {
        log.close();
        cache.clear();
    }

    public LogCache clear() {
        cache.clear();
        return this;
    }

    public Log trim() {
        final int oldestToRemove = cache.size() - max_size;
        if (oldestToRemove > 0) {
            cache.dropHeadUntil(cache.getHeadSequence() + oldestToRemove);
            num_trims++;
        }
        return this;
    }

    public String toString() {
        return String.format("first=%d commit=%d last=%d term=%d (%d entries, max-size=%d)",
                             first_appended, commit_index, last_appended, current_term, cache.size(), max_size);
    }

    public String toStringDetails() {
        return String.format("first=%d log-first=%d commit=%d log-commit=%d last=%d log-last=%d " +
                               "term=%d log-term=%d (max-size=%d)",
                             first_appended, log.firstAppended(), commit_index, log.commitIndex(),
                             last_appended, log.lastAppended(),  current_term, log.currentTerm(), max_size);
    }


}
