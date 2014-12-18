package org.jgroups.util;

/**
 * Quick hack to provide a Callable that also accepts a parameter. Will be removed when switching to JDK 8
 * @param <T> the type of the argument
 * @author Bela Ban
 * @since  0.1
 */
public interface Consumer<T> {
    void apply(T arg);
    void apply(Throwable t);
}
