package org.jgroups.raft.tests.serialization;

import java.util.Arrays;
import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class TestDataHolderProto {

    @ProtoField(1)
    final String id;

    @ProtoField(number = 2, defaultValue = "0")
    final long index;

    @ProtoField(number = 3, defaultValue = "0")
    final int term;

    @ProtoField(4)
    final byte[] data;

    @ProtoFactory
    public TestDataHolderProto(String id, long index, int term, byte[] data) {
        this.id = id;
        this.index = index;
        this.term = term;
        this.data = data;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass()) return false;
        TestDataHolderProto that = (TestDataHolderProto) object;
        return index == that.index
                && term == that.term
                && Objects.equals(id, that.id)
                && Objects.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index, term, Arrays.hashCode(data));
    }

    @Override
    public String toString() {
        return "TestDataHolderProto{" +
                "id='" + id + '\'' +
                ", index=" + index +
                ", term=" + term +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
