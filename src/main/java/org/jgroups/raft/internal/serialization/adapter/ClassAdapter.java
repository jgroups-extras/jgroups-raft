package org.jgroups.raft.internal.serialization.adapter;

import org.jgroups.raft.internal.serialization.ProtoStreamTypes;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(Class.class)
@ProtoTypeId(ProtoStreamTypes.CLASS_ADAPTER)
public class ClassAdapter {

    @ProtoFactory
    Class<?> create(String clazz) {
        try {
            return Class.forName(clazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Class not found: " + clazz, e);
        }
    }

    @ProtoField(1)
    String getClazz(Class<?> clazz) {
        return clazz.getName();
    }
}
