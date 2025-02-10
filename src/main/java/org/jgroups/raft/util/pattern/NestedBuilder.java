package org.jgroups.raft.util.pattern;

public interface NestedBuilder<T, P, B extends Builder<P>> extends Builder<T> {

    B and();
}
