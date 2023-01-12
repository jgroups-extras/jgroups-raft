package org.jgroups.raft.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Holds an immutable object behind a {@link ReadWriteLock}. All accesses to the underlying object
 * must acquire the proper lock. A simple approach to assert all access is executed through the same
 * synchronization mechanism. The lock is reentrant.
 * <p>
 * The access to the object is either a read or write. Calling the correct method, the client can use
 * a {@link Consumer} to access the object or a {@link Function} to compute a return value.
 *
 * @param <T> the type of object to hold.
 */
public class RWReference<T> {
    private final ReadWriteLock lock;
    private final T value;

    public RWReference(T value) {
        this.value = value;
        this.lock = new ReentrantReadWriteLock();
    }

    public void read(Consumer<T> consumer) {
        read(v -> {
            consumer.accept(v);
            return null;
        });
    }

    public void write(Consumer<T> consumer) {
        write(v -> {
            consumer.accept(v);
            return null;
        });
    }

    public <V> V read(Function<T, V> function) {
        Lock rl = lock.readLock();
        try {
            rl.lock();
            return function.apply(value);
        } finally {
            rl.unlock();
        }
    }

    public <V> V write(Function<T, V> function) {
        Lock wl = lock.writeLock();
        try {
            wl.lock();
            return function.apply(value);
        } finally {
            wl.unlock();
        }
    }
}
