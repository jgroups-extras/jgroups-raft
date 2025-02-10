package org.jgroups.tests.api;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
        allowNullFields = true,
        includeClasses = {
                KeyValueProto.class,
        },
        schemaFileName = "test-api.raft.proto",
        schemaFilePath = "proto/generated",
        schemaPackageName = "org.jgroups.raft.test",
        service = false,
        syntax = ProtoSyntax.PROTO3
)
public interface TestSerializationInitializer extends SerializationContextInitializer { }
