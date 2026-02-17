package org.jgroups.raft.internal.serialization;

import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.command.JGroupsRaftWriteCommandOptions;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.RaftCommand;
import org.jgroups.raft.internal.command.RaftResponse;
import org.jgroups.raft.internal.serialization.adapter.ClassAdapter;
import org.jgroups.raft.internal.statemachine.StateMachineStateHolder;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
        allowNullFields = true,
        includeClasses = {
                ObjectWrapper.class,
                RaftCommand.class,
                RaftResponse.class,
                JRaftCommand.UserCommand.class,
                JGroupsRaftWriteCommandOptions.WriteImpl.class,
                JGroupsRaftReadCommandOptions.ReadImpl.class,
                ClassAdapter.class,
                StateMachineStateHolder.class,
        },
        schemaFileName = "global.raft.proto",
        schemaFilePath = "proto/generated",
        schemaPackageName = RaftSerializationInitializer.PACKAGE_NAME,
        service = false,
        syntax = ProtoSyntax.PROTO3
)
public interface RaftSerializationInitializer extends SerializationContextInitializer {
    String PACKAGE_NAME = "org.jgroups.raft.internal";

    static String getFullTypeName(Class<?> clazz) {
        return PACKAGE_NAME + "." + clazz.getSimpleName();
    }
}
