package org.jgroups.tests.serialization;

import org.jgroups.raft.internal.serialization.RaftSerializationInitializer;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
        allowNullFields = true,
        dependsOn = {
                RaftSerializationInitializer.class
        },
        includeClasses = {
                TestDataHolderProto.class,
        },
        schemaFileName = "test.raft.proto",
        schemaFilePath = "proto/generated",
        schemaPackageName = "org.jgroups.raft.test",
        service = false,
        syntax = ProtoSyntax.PROTO3
)
public interface TestSerializationInitializer extends SerializationContextInitializer { }
