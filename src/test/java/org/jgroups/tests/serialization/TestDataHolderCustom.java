package org.jgroups.tests.serialization;

import java.util.Arrays;
import java.util.Objects;

public record TestDataHolderCustom(String id, long index, int term, byte[] data) {

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass()) return false;
        TestDataHolderCustom that = (TestDataHolderCustom) object;
        return index == that.index
                && term == that.term
                && Objects.equals(id, that.id)
                && Objects.deepEquals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index, term, Arrays.hashCode(data));
    }
}
