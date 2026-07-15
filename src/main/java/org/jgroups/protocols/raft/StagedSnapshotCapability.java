package org.jgroups.protocols.raft;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A log capability that allows snapshot creation in two phases: staging and committing.
 *
 * <p>
 * The caller writes snapshot data to the {@link OutputStream} returned by {@link #stage()}, then atomically promotes it
 * with {@link #commit(OutputStream)}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public interface StagedSnapshotCapability extends LogCapability {

    /**
     * Creates a new staging area for a snapshot and returns a stream to write into it.
     *
     * <p>
     * The returned stream must be closed before calling {@link #commit(OutputStream)}. Closing the stream flushes all data
     * to durable storage.
     * </p>
     *
     * @return an output stream for writing snapshot data
     * @throws IOException if the staging area cannot be created
     */
    OutputStream stage() throws IOException;

    /**
     * Atomically installs a previously staged snapshot as the active snapshot.
     *
     * <p><b>Must be called from the RAFT event loop.</b></p>
     *
     * @param staged the stream previously returned by {@link #stage()}, already closed
     * @throws IOException if the snapshot cannot be installed
     */
    void commit(OutputStream staged) throws IOException;
}
