package org.jgroups.raft.api;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;


public class KeyValueProto {
    private final String key;
    private final String value;

    @ProtoFactory
    public KeyValueProto(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @ProtoField(1)
    public String getKey() {
        return key;
    }

    @ProtoField(2)
    public String getValue() {
        return value;
    }
}
