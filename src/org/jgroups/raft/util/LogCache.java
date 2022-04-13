package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.ObjIntConsumer;

/**
 * Bounded caching {@link org.jgroups.protocols.raft.Log} facade. Reads are returned from the cache (if available),
 * writes invalidate the corresponding entries. Keeps the last N entries only (N is configurable).
 * @author Bela Ban
 * @since  1.0.8
 */
public class LogCache implements Log {
    protected final Log                            log;
    protected final NavigableMap<Integer,LogEntry> cache=new ConcurrentSkipListMap<>();
    protected int                                  max_size=1024;
    protected int                                  current_term, commit_index, first_appended, last_appended;
    protected Address                              voted_for;
    protected int                                  num_trims, num_hits, num_misses;


    public LogCache(Log log) {
        this.log=Objects.requireNonNull(log);
        current_term=log.currentTerm();
        commit_index=log.commitIndex();
        first_appended=log.firstAppended();
        last_appended=log.lastAppended();
        voted_for=log.votedFor();
    }

    public LogCache(Log log, int max_size) {
        this(log);
        this.max_size=max_size;
    }

    public int    maxSize()      {return max_size;}
    public Log    maxSize(int s) {max_size=s; return this;}
    public int    cacheSize()    {return cache.size();}
    public Log    log()          {return log;}
    public int    numTrims()     {return num_trims;}
    public int    numAccesses()  {return num_hits+num_misses;}
    public double hitRatio()     {return numAccesses() == 0? 0 : (double)num_hits / numAccesses();}
    public Log    resetStats()   {num_trims=num_hits=num_misses=0; return this;}


    public void init(String log_name, Map<String,String> args) throws Exception {
        log.init(log_name, args);
    }

    public void delete() throws Exception {
        log.delete();
        current_term=commit_index=first_appended=last_appended=0;
        voted_for=null;
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
        cache.headMap(new_index).clear();
        return this;
    }

    public int firstAppended() {
        return first_appended;
    }

    public int lastAppended() {
        return last_appended;
    }

    public void append(int index, boolean overwrite, LogEntry... entries) {
        log.append(index, overwrite, entries);
        last_appended=log.lastAppended();
        current_term=log.currentTerm();
        for(int i=0; i < entries.length; i++)
            cache.put(index++, entries[i]);
        trim();
    }

    public LogEntry get(int index) {
        if(index > last_appended) // called by every append() to check if the entry is already present
            return null;

        LogEntry e=cache.get(index);
        if(e != null) {
            num_hits++;
            return e;
        }
        num_misses++;
        e=log.get(index);
        return cache(index, e);
    }

    public void truncate(int index) {
        log.truncate(index);
        // todo: first_appended should be set to the return value of truncate() (once it has been changed)
        first_appended=log.firstAppended();
        cache.headMap(first_appended).clear();
    }

    public void deleteAllEntriesStartingFrom(int start_index) {
        log.deleteAllEntriesStartingFrom(start_index);
        commit_index=log.commitIndex();
        last_appended=log.lastAppended();
        current_term=log.currentTerm();
        cache.tailMap(start_index, true).clear();
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

    // todo: more efficient implementation
    public Log trim() {
        if(cache.size() > max_size) {
            int num_to_remove=cache.size() - max_size;
            Set<Integer> keys=cache.keySet();
            Iterator<Integer> it=keys.iterator();
            while(num_to_remove-- > 0 && it.hasNext()) {
                it.next();
                it.remove();
                num_trims++;
            }
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

    protected LogEntry cache(int index, LogEntry e) {
        if(e != null) {
            cache.put(index, e);
            trim();
        }
        return e;
    }


}
