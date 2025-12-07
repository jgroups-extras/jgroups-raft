package org.jgroups.raft.internal;

import org.jgroups.JChannel;
import org.jgroups.raft.JGroupsRaft;

public final class DangerousJGroupsRaftUtil {

    private DangerousJGroupsRaftUtil() { }

    public static JChannel extractJChannel(JGroupsRaft<?> raft) {
        if (raft instanceof JGroupsRaftImpl<?> impl) {
            return impl.channel();
        }

        return null;
    }
}
