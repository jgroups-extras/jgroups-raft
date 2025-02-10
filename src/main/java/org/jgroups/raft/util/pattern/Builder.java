package org.jgroups.raft.util.pattern;

public interface Builder<T> {

    default void validate() { }

    /**
     * Builds the object of type T.
     *
     * @return the built object
     */
    T build();
}
