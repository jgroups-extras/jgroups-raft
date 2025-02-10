package org.jgroups.raft.internal;

import org.jgroups.JChannel;
import org.jgroups.raft.configuration.RuntimeProperties;
import org.jgroups.raft.internal.registry.SerializationRegistry;
import org.jgroups.raft.logger.JRaftEventLogger;

record JGroupsRaftParameters<T>(String clusterName,
                                JChannel channel,
                                T sm,
                                Class<T> api,
                                SerializationRegistry registry,
                                JRaftEventLogger eventLogger,
                                RuntimeProperties runtimeProperties) {
}
