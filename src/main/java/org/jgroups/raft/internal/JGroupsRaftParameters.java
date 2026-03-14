package org.jgroups.raft.internal;

import org.jgroups.JChannel;
import org.jgroups.raft.configuration.RuntimeProperties;
import org.jgroups.raft.internal.serialization.binary.SerializationRegistry;

record JGroupsRaftParameters<T>(String clusterName,
                                JChannel channel,
                                T sm,
                                Class<T> api,
                                SerializationRegistry registry,
                                RuntimeProperties runtimeProperties) {
}
