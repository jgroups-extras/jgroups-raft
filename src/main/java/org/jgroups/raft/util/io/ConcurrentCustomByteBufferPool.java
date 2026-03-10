package org.jgroups.raft.util.io;

import java.util.concurrent.atomic.AtomicReferenceArray;

public class ConcurrentCustomByteBufferPool {

    /**
     * Array of buffer slots. null = empty slot, non-null = available buffer.
     */
    private final AtomicReferenceArray<CustomByteBuffer> slots;

    /**
     * Maximum size of individual buffer to cache.
     */
    private final int maxBufferSize;

    /**
     * Creates a pool with default settings.
     * <ul>
     *   <li>Pool capacity: 16 buffers</li>
     *   <li>Max buffer size: 64 KB</li>
     * </ul>
     */
    public ConcurrentCustomByteBufferPool() {
        this(16, 64 * 1024);
    }

    /**
     * Creates a pool with specified limits.
     *
     * @param poolCapacity Maximum number of buffers to cache
     * @param maxBufferSize Maximum size of individual buffer to cache (bytes)
     */
    public ConcurrentCustomByteBufferPool(int poolCapacity, int maxBufferSize) {
        if (poolCapacity <= 0) {
            throw new IllegalArgumentException("poolCapacity must be positive: " + poolCapacity);
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive: " + maxBufferSize);
        }

        this.slots = new AtomicReferenceArray<>(poolCapacity);
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Acquires a buffer with at least the requested capacity.
     *
     * <p>
     * This method scans the pool for a suitable buffer using lock-free CAS operations.
     * If multiple threads compete for the same buffer, one wins via CAS and others retry.
     * </p>
     *
     * <p>
     * <b>Algorithm:</b>
     * </p>
     * <ol>
     *   <li>Scan slots for non-null buffer with capacity &gt;= requestedSize</li>
     *   <li>Attempt CAS to acquire buffer (set slot to null)</li>
     *   <li>If CAS succeeds, return buffer; if fails, continue scanning</li>
     *   <li>If no suitable buffer found, allocate new one</li>
     * </ol>
     *
     * @param requestedSize Minimum buffer capacity needed
     * @return A buffer with capacity &gt;= requestedSize
     */
    public CustomByteBuffer acquire(int requestedSize) {
        int length = slots.length();

        // First pass: look for exact or larger buffer
        CustomByteBuffer buffer = tryAcquire(requestedSize, length);
        if (buffer != null)
            return buffer;

        // Second pass: if requestedSize is large, try any available buffer
        // (caller might be okay with a smaller buffer if they can't get exact size)
        // Actually, skip this - just allocate new buffer

        // No suitable buffer in pool, allocate new
        return CustomByteBuffer.allocate(requestedSize);
    }

    /**
     * Returns a buffer to the pool.
     *
     * <p>
     * The buffer may not be cached if:
     * </p>
     * <ul>
     *   <li>Its capacity exceeds {@code maxBufferSize}</li>
     *   <li>All slots are full and no smaller buffer can be replaced</li>
     * </ul>
     *
     * <p>
     * <b>Algorithm:</b>
     * </p>
     * <ol>
     *   <li>Try to CAS buffer into an empty (null) slot</li>
     *   <li>If no empty slots, try to replace a smaller buffer</li>
     *   <li>If pool is full with larger/equal buffers, discard (will be GC'd)</li>
     * </ol>
     *
     * @param buffer The buffer to return (can be null)
     */
    public void release(CustomByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        int capacity = buffer.capacity();

        // Don't cache buffers that are too large
        if (capacity > maxBufferSize) {
            return;
        }

        int length = slots.length();

        // First pass: try to find an empty slot
        for (int i = 0; i < length; i++) {
            if (slots.compareAndSet(i, null, buffer)) {
                // Successfully cached in empty slot
                return;
            }
        }

        // Second pass: try to replace a smaller buffer
        for (int i = 0; i < length; i++) {
            CustomByteBuffer existing = slots.get(i);

            if (existing != null && existing.capacity() < capacity && slots.compareAndSet(i, existing, buffer)) {
                // Successfully replaced smaller buffer
                return;
            }
        }

        // Pool is full with larger/equal buffers, discard this buffer
        // It will be GC'd
    }

    private CustomByteBuffer tryAcquire(int requestedSize, int length) {
        for (int i = 0; i < length; i++) {
            CustomByteBuffer buffer = slots.get(i);

            if (buffer != null
                    && buffer.capacity() >= requestedSize
                    && slots.compareAndSet(i, buffer, null)) {
                buffer.clear();
                return buffer;
            }
        }
        return null;
    }

    /**
     * Clears all buffers from the pool.
     */
    public void clear() {
        int length = slots.length();
        for (int i = 0; i < length; i++) {
            slots.set(i, null);
        }
    }

    /**
     * Returns the number of buffers currently in the pool.
     *
     * <p>
     * <b>Note:</b> This is a snapshot value and may be stale by the time it's returned
     * due to concurrent modifications.
     * </p>
     */
    public int size() {
        int count = 0;
        int length = slots.length();

        for (int i = 0; i < length; i++) {
            if (slots.get(i) != null) {
                count++;
            }
        }

        return count;
    }

    /**
     * Returns the pool capacity (maximum number of buffers that can be cached).
     */
    public int capacity() {
        return slots.length();
    }

    /**
     * Returns the maximum buffer size that will be cached.
     */
    public int maxBufferSize() {
        return maxBufferSize;
    }
}
