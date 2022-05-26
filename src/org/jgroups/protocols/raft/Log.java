package org.jgroups.protocols.raft;

import org.jgroups.Address;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.ObjLongConsumer;

/**
 * The interface for a persistent log. See doc/design/Log.txt for details.
 * @author Bela Ban
 * @since  0.1
 */
public interface Log extends Closeable {

    /** Called after the instance has been created
     * @param log_name The name of the log. Implementations can create a DB or file named after this, e.g.
     *                 /tmp/<log_name></log_name>.log
     * @param args A hashmap of configuration information (impl-dependent) to configure itself. May be null
     */
    void init(String log_name, Map<String,String> args) throws Exception;

    /**
     * Do not cache a change (e.g. AppendEntriesRequest, or setting the commit index), but force a write
     * to disk (fsync), if true.
     */
    Log useFsync(boolean f);

    boolean useFsync();

    /** Remove the persistent store, e.g. DB table, or file */
    void delete() throws Exception;

    /** Returns the current term */
    long currentTerm();

    /** Sets the current term */
    Log currentTerm(long new_term);

    /** Returns the address of the candidate that this node voted for in the current term */
    Address votedFor();

    /** Sets the address of the member this node voted for in the current term. Only invoked once per term */
    Log votedFor(Address member);

    /** Returns the current commit index. (May get removed as the RAFT paper has this as in-memory attribute) */
    long commitIndex();

    /**
     * Sets commitIndex to a new value
     * @param new_index The new index to set commitIndex to. May throw an exception if new_index > lastApplied()
     * @return the log
     */
    Log commitIndex(long new_index);

    /** Returns the index of the first log entry */
    long firstAppended();

    /** Returns the index of the last append entry<br/>
     * This value is set by {@link #append(long,LogEntries)} */
    long lastAppended();

    /**
     * Stores a snapshot in the log.
     * @param sn The snapshot data
     */
    void setSnapshot(ByteBuffer sn);

    /**
     * Gets the snapshot from the log
     * @return The snapshot, or null if not existing
     */
    ByteBuffer getSnapshot();

    /**
     * Append the entries starting at index. Advance last_appended by the number of entries appended.
     * <br/>
     * If the operation fails, then last_appended needs to be the index of the last successful append. E.g. if
     * last_appended is 1, and we attempt to appened 100 entries, but fail at 51, then last_appended must be 50 (not 1!).
     * <br/>
     * @param index The index at which to append the entries. Should be the same as lastAppended. LastAppended needs
     *              to be incremented by the number of entries appended
     * @param entries The entries to append
     * @return long The index of the last appended entry
     */
    long append(long index, LogEntries entries);


    /**
     * Gets the entry at start_index. Updates current_term and last_appended accordingly
     * @param index The index
     * @return The LogEntry, or null if none is present at index.
     */
    LogEntry get(long index);

    /**
     * Truncates the log up to (and excluding) index. All entries < index are removed. First = index.
     * @param index_exclusive If greater than commit_index, commit_index will be used instead
     */
    void truncate(long index_exclusive);

    /**
     * Clears all entries and sets first_appended/last_appended/commit_index to index and appends entry at index. The
     * next entry will be appended at last_appended+1.<br/>
     * Use when a snapshot has been received by a follower, after setting the snapshot, to basically create a new log
     * @param index The new index
     * @param entry The entry to append
     * @throws Exception Thrown if this operation failed
     */
    void reinitializeTo(long index, LogEntry entry) throws Exception;

    /**
     * Delete all entries starting from start_index (including the entry at start_index).
     * Updates current_term and last_appended accordingly
     *
     * @param start_index
     */
    void deleteAllEntriesStartingFrom(long start_index);


    /**
     * Applies function to all elements of the log in range [max(start_index,first_appended) .. min(last_appended,end_index)].
     * @param function The function to be applied
     * @param start_index The start index. If smaller than first_appended, first_appended will be used
     * @param end_index The end index. If greater than last_appended, last_appended will be used
     */
    void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index);

    /** Applies a function to all elements in range [first_appended .. last_appended] */
    void forEach(ObjLongConsumer<LogEntry> function);

    /** The number of entries in the log */
    default long size() {
        long last=lastAppended(), first=firstAppended();
        return first == 0? last : last-first+1;
    }

    long sizeInBytes();
}
