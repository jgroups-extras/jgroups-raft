package org.jgroups.raft.blocks;

import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.raft.Options;

/**
 * TODO! document this
 */
public interface RaftAsyncCounter extends AsyncCounter, RaftCounter {

    @Override
    default RaftAsyncCounter async() {
        return this;
    }

    @Override
    RaftAsyncCounter withOptions(Options opts);
}
