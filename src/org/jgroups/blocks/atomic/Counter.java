package org.jgroups.blocks.atomic;

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

}
