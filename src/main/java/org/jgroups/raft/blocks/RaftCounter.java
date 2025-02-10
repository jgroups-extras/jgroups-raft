package org.jgroups.raft.blocks;

import org.jgroups.blocks.atomic.BaseCounter;
import org.jgroups.raft.Options;

/**
 * TODO! document this
 */
public interface RaftCounter extends BaseCounter {

    /**
     * Gets the current local value of the counter; this is purely local and the value may be stale
     *
     * @return The current local value of the counter
     */
    long getLocal();

    /**
     * Returns an instance of a counter with the given options
     *
     * @param opts The options
     * @return The counter of the given type
     */
    RaftCounter withOptions(Options opts);

    @Override
    RaftSyncCounter sync();

    @Override
    RaftAsyncCounter async();
}
