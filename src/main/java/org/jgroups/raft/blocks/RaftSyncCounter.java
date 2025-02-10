package org.jgroups.raft.blocks;

import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.raft.Options;

/**
 * TODO! document this
 */
public interface RaftSyncCounter extends SyncCounter, RaftCounter {

    @Override
    default RaftSyncCounter sync() {
        return this;
    }

    @Override
    RaftSyncCounter withOptions(Options opts);
}
