package org.jgroups.protocols.raft;

import org.jgroups.Address;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.ObjLongConsumer;

/**
 * Durable storage for Raft consensus log entries, election metadata (term and vote), and state machine snapshots.
 *
 * <p>
 * Implementations provide the persistence layer that backs the Raft consensus algorithm. Each Raft node owns exactly one
 * {@code Log} instance for the lifetime of the node. The log stores three categories of data:
 * </p>
 *
 * <ul>
 *   <li><b>Entries</b>: an ordered, 1-based sequence of {@link LogEntry} records appended during replication.</li>
 *   <li><b>Election metadata</b>: the current term ({@link #currentTerm(long)}) and the candidate voted for in that
 *       term ({@link #votedFor(Address)}). These values must survive restarts to preserve Raft's single-vote-per-term invariant.</li>
 *   <li><b>Snapshot</b>: a point-in-time capture of the state machine, used to compact the log.</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 *
 * <p>
 * A {@code Log} instance must be {@linkplain #init(String, Map) initialized} before any other method is called and
 * {@linkplain #close() closed} when no longer needed. After {@code close()}, behavior of all other methods is undefined.
 * </p>
 *
 * <h2>Durability and failure</h2>
 *
 * <p>
 * Mutating operations declare {@link IOException} to signal storage failures. When an {@code IOException} is thrown, the
 * operation did not complete and the log's state may be inconsistent. Callers must not assume partial progress unless a
 * method's contract explicitly defines it (see {@link #append(long, LogEntries)}).
 * </p>
 *
 * <p>
 * When {@link #useFsync(boolean) fsync} is enabled, each mutating operation guarantees that data has reached stable storage
 * before the call returns.
 * </p>
 *
 * <h2>Thread safety</h2>
 *
 * <p>
 * Implementations may assume <b>single-writer</b> semantics: at most one thread performs mutating operations at any given
 * time. However, read-only accessors ({@link #currentTerm()}, {@link #votedFor()}, {@link #commitIndex()},
 * {@link #firstAppended()}, {@link #lastAppended()}) must be safe to call concurrently from any thread while a mutating
 * operation is in progress.
 * </p>
 *
 * <h2>Delegation</h2>
 *
 * <p>
 * Implementations that wrap another {@code Log} (e.g., caching or adapter layers) should delegate {@link #findCapability(Class)}
 * to the wrapped log so that the full delegation chain is discoverable.
 * </p>
 *
 * @author Bela Ban
 * @since 0.1
 * @see LogEntry
 * @see LogCapability
 */
public interface Log extends Closeable {

    /**
     * Initializes this log with the given identity and optional configuration. Must be called exactly once
     * before any other method.
     *
     * @param log_name a name used to derive the storage identity (file name, database table, etc.).
     *                 Two logs initialized with the same name in the same environment share state.
     * @param args implementation-specific configuration parameters, may be {@code null}
     * @throws Exception if initialization fails (missing directory, permission denied, corrupt storage, etc.)
     */
    void init(String log_name, Map<String,String> args) throws Exception;

    /**
     * Controls whether mutating operations force data to stable storage (fsync) before returning.
     *
     * @param f {@code true} to force all writes to stable storage
     * @return this log
     */
    Log useFsync(boolean f);

    /**
     * Returns whether writes are forced to stable storage before mutating operations return.
     *
     * @return {@code true} if fsync is enabled
     */
    boolean useFsync();

    /**
     * Returns the current Raft term.
     *
     * @return the current term, or 0 if not yet set
     */
    long currentTerm();

    /**
     * Persists the current Raft term. The Raft protocol guarantees that terms are monotonically increasing.
     *
     * @param new_term the term to persist
     * @return this log
     * @throws IOException if the term cannot be persisted
     */
    Log currentTerm(long new_term) throws IOException;

    /**
     * Returns the candidate this node voted for in the current term.
     *
     * @return the address of the voted-for candidate, or {@code null} if no vote has been cast in this term
     */
    Address votedFor();

    /**
     * Persists the vote for the given candidate in the current term. The Raft protocol guarantees at most
     * one vote per term. Passing {@code null} clears the vote, typically at term boundaries.
     *
     * @param member the candidate address, or {@code null} to clear the vote
     * @return this log
     * @throws IOException if the vote cannot be persisted
     */
    Log votedFor(Address member) throws IOException;

    /**
     * Returns the current commit index.
     *
     * @return the commit index, or 0 if no entries have been committed
     */
    long commitIndex();

    /**
     * Persists the commit index.
     *
     * @param new_index the new commit index
     * @return this log
     * @throws IOException if the commit index cannot be persisted
     */
    Log commitIndex(long new_index) throws IOException;

    /**
     * Returns the index of the first entry in the log.
     *
     * @return the first entry index, or 0 if the log is empty
     */
    long firstAppended();

    /**
     * Returns the index of the last appended entry.
     *
     * @return the last appended index, or 0 if the log is empty
     */
    long lastAppended();

    /**
     * Stores a snapshot of the state machine, replacing any previously stored snapshot.
     *
     * @param sn the snapshot data
     * @throws IOException if the snapshot cannot be stored
     */
    void setSnapshot(ByteBuffer sn) throws IOException;

    /**
     * Returns the most recently stored snapshot.
     *
     * @return the snapshot data, or {@code null} if no snapshot has been stored
     * @throws IOException if the snapshot cannot be read
     */
    ByteBuffer getSnapshot() throws IOException;

    /**
     * Appends entries starting at the given index and advances {@link #lastAppended()} accordingly.
     *
     * <p>
     * On partial failure, {@code lastAppended} must reflect the last successfully written entry rather than
     * reverting to the value before the call. For example, if {@code lastAppended} was 1 and 100 entries were
     * submitted but the operation failed at entry 51, {@code lastAppended} must be 50 after the exception.
     * </p>
     *
     * @param index the index at which to begin appending
     * @param entries the entries to append
     * @return the index of the last appended entry
     * @throws IOException if one or more entries cannot be written to storage
     */
    long append(long index, LogEntries entries) throws IOException;

    /**
     * Returns the entry at the given index.
     *
     * @param index the log index to retrieve
     * @return the entry, or {@code null} if no entry exists at that index
     * @throws IOException if the entry cannot be read from storage
     */
    LogEntry get(long index) throws IOException;

    /**
     * Removes all entries before the given index. After this call, {@link #firstAppended()} equals
     * {@code index_exclusive}.
     *
     * <p>
     * If {@code index_exclusive} exceeds {@link #commitIndex()}, the commit index is used instead to
     * prevent truncating committed entries.
     * </p>
     *
     * @param index_exclusive entries strictly before this index are removed
     * @throws IOException if the truncation cannot be completed
     */
    void truncate(long index_exclusive) throws IOException;

    /**
     * Resets the log to contain only the given entry at the given index. Sets {@link #firstAppended()},
     * {@link #lastAppended()}, and {@link #commitIndex()} to {@code index}. The next entry will be appended
     * at {@code index + 1}.
     *
     * <p>
     * Typically called after installing a snapshot received from the leader.
     * </p>
     *
     * @param index the new starting index
     * @param entry the single entry to store
     * @throws IOException if the log cannot be reinitialized
     */
    void reinitializeTo(long index, LogEntry entry) throws IOException;

    /**
     * Deletes all entries at and after {@code start_index}. Updates {@link #lastAppended()} and
     * {@link #currentTerm()} to reflect the remaining log.
     *
     * @param start_index the first index to delete (inclusive)
     * @throws IOException if the deletion cannot be completed
     */
    void deleteAllEntriesStartingFrom(long start_index) throws IOException;

    /**
     * Applies the function to each entry in the range
     * [{@code max(start_index, firstAppended)} .. {@code min(end_index, lastAppended)}], inclusive.
     *
     * @param function invoked with each entry and its index
     * @param start_index the start of the range (inclusive), clamped to {@link #firstAppended()}
     * @param end_index the end of the range (inclusive), clamped to {@link #lastAppended()}
     * @throws IOException if an entry cannot be read
     */
    void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) throws IOException;

    /**
     * Applies the function to every entry in the log. Equivalent to
     * {@code forEach(function, firstAppended(), lastAppended())}.
     *
     * @param function invoked with each entry and its index
     * @throws IOException if an entry cannot be read
     */
    void forEach(ObjLongConsumer<LogEntry> function) throws IOException;

    /**
     * Returns the number of entries in the log.
     *
     * @return the entry count
     */
    default long size() {
        long last=lastAppended(), first=firstAppended();
        return first == 0? last : last-first+1;
    }

    /**
     * Returns the approximate size of the log data in bytes.
     *
     * @return the size in bytes
     */
    long sizeInBytes();

    /**
     * Looks up an optional capability provided by this log or any log in the delegation chain.
     *
     * <p>
     * Implementations that wrap another {@link Log} should delegate to the wrapped log when they do not provide the
     * requested capability themselves.
     * </p>
     *
     * @param capability the capability interface to look up
     * @param <T> the capability type
     * @return the capability instance, or {@code null} if not available
     */
    default <T extends LogCapability> T findCapability(Class<T> capability) {
        return null;
    }
}
