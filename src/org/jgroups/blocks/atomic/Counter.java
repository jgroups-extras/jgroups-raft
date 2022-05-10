package org.jgroups.blocks.atomic;

import org.jgroups.raft.Options;

/**
 * Base interface for all counters
 * @author Bela Ban
 * @since  1.0.9
 */
public interface Counter {
    /**
     * @return The counter's name.
     */
    String getName();

    /**
     * Returns a {@link SyncCounter} wrapper for this instance. If this counter is already synchronous, then this
     * counter instance is returned (no-op)
     * @return SyncCounter A SyncCounter
     */
    SyncCounter sync();

    /**
     * Returns an {@link AsyncCounter} wrapper for this instance. If this counter is already asynchronous, then
     * this counter instance is returned (no-op)
     * @return
     */
    AsyncCounter  async();

    /**
     * Returns an instance of a counter with the given options
     * @param opts The options
     * @param <T> The type of the counter, e.g. {@link AsyncCounter} or {@link SyncCounter}
     * @return The counter of the given type
     */
    <T extends Counter> T withOptions(Options opts);

}
