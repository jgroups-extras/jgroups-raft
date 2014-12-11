package org.jgroups.util;

/**
 * Quick hack to provide a Callable that also accepts a parameter. Will be removed when switching to JDK 8
 * @param <T> the type of the argument
 * @param <R> the type of the return value
 * @author Bela Ban
 * @since  0.1
 */
public interface Function<T,R> {
    R apply(T arg);
    R apply(Throwable t);
}
