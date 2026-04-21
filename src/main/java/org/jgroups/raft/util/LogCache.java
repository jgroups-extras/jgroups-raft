package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.LogCapability;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogCacheControl;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.function.ObjLongConsumer;

/**
 * Bounded caching {@link org.jgroups.protocols.raft.Log} facade. Reads are returned from the cache (if available),
 * writes invalidate the corresponding entries. Keeps the last N entries only (N is configurable).
 *
 * @author Bela Ban
 * @since 1.0.8
 */
public final class LogCache implements Log, LogCacheControl {
    private static final int DEFAULT_MAX_SIZE = 1024;
    private final Log log;
    private ArrayRingBuffer<LogEntry> cache;
    private int max_size;
    private long current_term, commit_index, first_appended, last_appended;
    private Address voted_for;
    private int num_trims, num_hits, num_misses;
    private boolean passthrough;


    public LogCache(Log log) {
        this(log, DEFAULT_MAX_SIZE);
    }

    public LogCache(Log log, int max_size) {
        this.log = Objects.requireNonNull(log);
        current_term = log.currentTerm();
        commit_index = log.commitIndex();
        first_appended = log.firstAppended();
        last_appended = log.lastAppended();
        voted_for = log.votedFor();
        this.max_size = max_size;
        cache = new ArrayRingBuffer<>(max_size, log.commitIndex());
    }

    @Override
    public int maxSize() {
        return max_size;
    }

    @Override
    public void maxSize(int size) {
        this.max_size = size;
    }

    @Override
    public int cacheSize() {
        return cache.size();
    }

    @Override
    public int numTrims() {
        return num_trims;
    }

    @Override
    public int numAccesses() {
        return num_hits + num_misses;
    }

    @Override
    public double hitRatio() {
        return numAccesses() == 0 ? 0 : (double) num_hits / numAccesses();
    }

    @Override
    public String description() {
        if (passthrough) {
            return String.format("%s (passthrough) -> %s", getClass().getSimpleName(), log.getClass().getSimpleName());
        }
        return toStringDetails();
    }

    @Override
    public void resetStats() {
        num_trims = num_hits = num_misses = 0;
    }

    @Override
    public Log useFsync(boolean f) {
        log.useFsync(f);
        return this;
    }

    @Override
    public boolean useFsync() {
        return log.useFsync();
    }

    @Override
    public void init(String log_name, Map<String, String> args) throws Exception {
        log.init(log_name, args);
    }

    @Override
    public long currentTerm() {
        return current_term;
    }

    @Override
    public Log currentTerm(long new_term) throws IOException {
        log.currentTerm(new_term);
        current_term = new_term; // if the above fails, current_term won't be set
        return this;
    }

    @Override
    public Address votedFor() {
        return voted_for;
    }

    @Override
    public Log votedFor(Address member) throws IOException {
        log.votedFor(member);
        voted_for = member;
        return this;
    }

    @Override
    public long commitIndex() {
        return commit_index;
    }

    @Override
    public Log commitIndex(long new_index) throws IOException {
        log.commitIndex(new_index);
        commit_index = new_index;
        if (!passthrough)
            cache.dropHeadUntil(new_index);
        return this;
    }

    @Override
    public long firstAppended() {
        return first_appended;
    }

    @Override
    public long lastAppended() {
        return last_appended;
    }

    @Override
    public ByteBuffer getSnapshot() throws IOException {
        return log.getSnapshot();
    }

    @Override
    public void setSnapshot(ByteBuffer sn) throws IOException {
        log.setSnapshot(sn); // the LogCache doesn't cache snapshots; this operation isn't frequent anyway
    }

    @Override
    public long append(long index, LogEntries entries) throws IOException {
        last_appended = log.append(index, entries);
        current_term = log.currentTerm();
        if (passthrough)
            return last_appended;

        for (LogEntry le : entries) {
            final long logIndex = index++;
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

    @Override
    public LogEntry get(long index) throws IOException {
        if (passthrough)
            return log.get(index);

        if (index > last_appended) // called by every append() to check if the entry is already present
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

    @Override
    public void truncate(long index_exclusive) throws IOException {
        log.truncate(index_exclusive);
        // todo: first_appended should be set to the return value of truncate() (once it has been changed)
        first_appended = log.firstAppended();
        last_appended = log.lastAppended();
        if (!passthrough)
            cache.dropHeadUntil(index_exclusive);
    }

    @Override
    public void reinitializeTo(long index, LogEntry le) throws IOException {
        log.reinitializeTo(index, le);
        first_appended = log.firstAppended();
        commit_index = log.commitIndex();
        last_appended = log.lastAppended();
        current_term = log.currentTerm();

        if (!passthrough) {
            cache.clear();
            cache = new ArrayRingBuffer<>(max_size, index);
            cache.add(le);
        }
    }

    @Override
    public void deleteAllEntriesStartingFrom(long start_index) throws IOException {
        log.deleteAllEntriesStartingFrom(start_index);
        commit_index = log.commitIndex();
        last_appended = log.lastAppended();
        current_term = log.currentTerm();

        if (!passthrough)
            cache.dropTailTo(start_index);
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) throws IOException {
        if (passthrough) {
            log.forEach(function, start_index, end_index);
            return;
        }

        long from = Math.max(start_index, Math.max(first_appended, 1));
        long to = Math.min(end_index, last_appended);
        for (long i = from; i <= to; i++) {
            LogEntry l = get(i);
            function.accept(l, i);
        }
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function) throws IOException {
        forEach(function, first_appended, last_appended);
    }

    @Override
    public long sizeInBytes() {
        return log.sizeInBytes();
    }

    @Override
    public void close() throws IOException {
        log.close();
        cache.clear();
    }

    @Override
    public void enable(int maxSize) {
        this.max_size = maxSize;
        passthrough = false;
    }

    @Override
    public void disable() {
        cache.clear();
        passthrough = true;
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void trim() {
        if (passthrough) {
            return;
        }

        final int oldestToRemove = cache.size() - max_size;
        if (oldestToRemove > 0) {
            cache.dropHeadUntil(cache.getHeadSequence() + oldestToRemove);
            num_trims++;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends LogCapability> T findCapability(Class<T> capability) {
        if (capability.isAssignableFrom(LogCacheControl.class))
            return (T) this;

        return log.findCapability(capability);
    }

    @Override
    public String toString() {
        return String.format("first=%d commit=%d last=%d term=%d (%d entries, max-size=%d)",
                first_appended, commit_index, last_appended, current_term, cache.size(false), max_size);
    }

    public String toStringDetails() {
        return String.format("first=%d log-first=%d commit=%d log-commit=%d last=%d log-last=%d " +
                        "term=%d log-term=%d (max-size=%d)",
                first_appended, log.firstAppended(), commit_index, log.commitIndex(),
                last_appended, log.lastAppended(), current_term, log.currentTerm(), max_size);
    }
}
