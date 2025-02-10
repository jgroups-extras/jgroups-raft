package org.jgroups.raft.internal;

import org.jgroups.JChannel;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.configuration.RuntimeProperties;
import org.jgroups.raft.internal.registry.SerializationRegistry;
import org.jgroups.raft.logger.JRaftEventLogger;

public final class JGroupsRaftFactory {

    private JGroupsRaftFactory() { }

    public static <T> JGroupsRaft<T> create(
            String clusterName,
            JChannel channel,
            T sm,
            Class<T> api,
            SerializationRegistry registry,
            JRaftEventLogger eventLogger,
            RuntimeProperties runtimeProperties) {
        return new JGroupsRaftImpl<>(new JGroupsRaftParameters<>(
                clusterName,
                channel,
                sm,
                api,
                registry,
                eventLogger,
                runtimeProperties));
    }
}
