package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ObjLongConsumer;

/**
 * Boundary between the Raft protocol and persistent storage.
 *
 * <p>
 * Wraps any {@link Log} implementation and absorbs storage failures so that the protocol layer sees either a healthy log
 * or a clean {@link RaftLogException}. This allows a clear separation of concerns. The Raft implementation continues to
 * treat the persistent storage as perfect (expected in the algorithm definition and dissertation), at the same time, a
 * persistent storage is allowed to fail and throw exceptions, instead of failing silently.
 * </p>
 *
 * <p>
 * Before poisoning, all operations delegate transparently to the wrapped log. On the first storage failure, the adapter
 * stores the cause, notifies the registered {@link LogFailureListener}, and logs an ERROR. All subsequent storage-mutating
 * operations throw the stored {@link RaftLogException}. Read-only status accessors ({@link #currentTerm()},
 * {@link #commitIndex()}, {@link #lastAppended()}, etc.) continue to delegate normally.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see LogFailureListener
 * @see RaftLogException
 */
final class RaftLogAdapter implements Log, LogCapability {

    private static final org.jgroups.logging.Log LOG = LogFactory.getLog(RaftLogAdapter.class);

    private final Log delegate;
    private final LogFailureListener listener;
    private final AtomicReference<RaftLogException> cause = new AtomicReference<>(null);

    public RaftLogAdapter(Log delegate, LogFailureListener listener) {
        this.delegate = delegate;
        this.listener = listener;
    }

    /**
     * Poisons this adapter, causing all subsequent storage-mutating operations to throw.
     *
     * <p>
     * Only the first invocation has effect, all subsequent calls are ignored.
     * </p
     *
     * @param c the underlying storage failure
     */
    RaftLogException poison(Throwable c) {
        RaftLogException ex = new RaftLogException(String.format("(%s) storage failure: %s", delegate.getClass(), c.getMessage()), c);
        if (cause.compareAndSet(null, ex)) {
            LOG.error("Storage failed, entering into degraded mode", ex);
            listener.onLogFailure(c);
        }
        return cause.get();
    }

    public boolean isPoisoned() {
        return cause.get() != null;
    }

    private void assertNotPoisoned() {
        RaftLogException ex = cause.get();
        if (ex != null)
            throw ex;
    }

    @Override
    public void init(String log_name, Map<String, String> args) throws Exception {
        assertNotPoisoned();
        delegate.init(log_name, args);
    }

    @Override
    public Log useFsync(boolean f) {
        delegate.useFsync(f);
        return this;
    }

    @Override
    public boolean useFsync() {
        return delegate.useFsync();
    }

    @Override
    public long currentTerm() {
        return delegate.currentTerm();
    }

    @Override
    public Log currentTerm(long new_term) {
        assertNotPoisoned();
        try {
            delegate.currentTerm(new_term);
            return this;
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public Address votedFor() {
        return delegate.votedFor();
    }

    @Override
    public Log votedFor(Address member) {
        assertNotPoisoned();
        try {
            delegate.votedFor(member);
            return this;
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public long commitIndex() {
        return delegate.commitIndex();
    }

    @Override
    public Log commitIndex(long new_index) {
        assertNotPoisoned();
        try {
            delegate.commitIndex(new_index);
            return this;
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public long firstAppended() {
        return delegate.firstAppended();
    }

    @Override
    public long lastAppended() {
        return delegate.lastAppended();
    }

    @Override
    public void setSnapshot(ByteBuffer sn) {
        assertNotPoisoned();
        try {
            delegate.setSnapshot(sn);
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public ByteBuffer getSnapshot() {
        assertNotPoisoned();
        try {
            return delegate.getSnapshot();
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public long append(long index, LogEntries entries) {
        assertNotPoisoned();
        try {
            return delegate.append(index, entries);
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public LogEntry get(long index) {
        assertNotPoisoned();
        try {
            return delegate.get(index);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void truncate(long index_exclusive) {
        assertNotPoisoned();
        try {
            delegate.truncate(index_exclusive);
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public void reinitializeTo(long index, LogEntry entry) {
        assertNotPoisoned();
        try {
            delegate.reinitializeTo(index, entry);
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public void deleteAllEntriesStartingFrom(long start_index) {
        assertNotPoisoned();
        try {
            delegate.deleteAllEntriesStartingFrom(start_index);
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) {
        assertNotPoisoned();
        try {
            delegate.forEach(function, start_index, end_index);
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function) {
        assertNotPoisoned();
        try {
            delegate.forEach(function);
        } catch (IOException e) {
            throw poison(e);
        }
    }

    @Override
    public long sizeInBytes() {
        return delegate.sizeInBytes();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public <T extends LogCapability> T findCapability(Class<T> capability) {
        return delegate.findCapability(capability);
    }

    @Override
    public String toString() {
        String state = isPoisoned() ? "POISONED" : "healthy";
        return String.format("RaftLogAdapter(%s) -> %s", state, delegate.getClass().getSimpleName());
    }
}
