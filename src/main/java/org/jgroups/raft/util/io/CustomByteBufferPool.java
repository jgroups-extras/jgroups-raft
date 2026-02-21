package org.jgroups.raft.util.io;

/**
 * Pool of buffers limited by memory size.
 *
 * <p>
 * This pool maintains a small cache of buffers (default: 4) to handle nested or sequential serialization calls within
 * the same thread. It uses a simple array-based design with no synchronization overhead.
 * </p>
 *
 * <p>
 * <b>Warning:</b> This implementation is not thread-safe.
 * </p>
 *
 * <p>
 * Memory usage per thread: {@code maxBuffers * maxBufferSize}. Default: 4 * 64KB = 256KB max per thread.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public class CustomByteBufferPool {

    /**
     * Simple slot to hold a buffer and track if it's in use.
     */
    private static final class BufferSlot {
        CustomByteBuffer buffer;
        boolean inUse;

        BufferSlot() {
            this.buffer = null;
            this.inUse = false;
        }

        public boolean acquire(int size) {
            if (inUse || buffer == null)
                return false;

            if (buffer.capacity() < size)
                return false;

            inUse = true;
            return true;
        }

        public boolean release(CustomByteBuffer buffer) {
            if (this.buffer != buffer)
                return false;

            inUse = false;
            buffer.clear();
            return true;
        }

        public boolean tryReplace(CustomByteBuffer buffer) {
            if (!replaceable(buffer))
                return false;

            assert !inUse;
            this.buffer = buffer.clear();
            return true;
        }

        public boolean replaceable(CustomByteBuffer other) {
            if (inUse) return false;

            return buffer == null || buffer.capacity() < other.capacity();
        }
    }

    private final BufferSlot[] slots;
    private final int maxBufferSize;

    /**
     * Creates a pool with default settings.
     * <ul>
     *   <li>Max buffers: 4</li>
     *   <li>Max buffer size: 64 KB</li>
     * </ul>
     */
    public CustomByteBufferPool() {
        this(4, 64 * 1024);
    }

    /**
     * Creates a pool with specified limits.
     *
     * @param maxBuffers Maximum number of buffers to cache
     * @param maxBufferSize Maximum size of individual buffer to cache (bytes)
     */
    public CustomByteBufferPool(int maxBuffers, int maxBufferSize) {
        if (maxBuffers <= 0) {
            throw new IllegalArgumentException("maxBuffers must be positive: " + maxBuffers);
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive: " + maxBufferSize);
        }

        this.slots = new BufferSlot[maxBuffers];
        for (int i = 0; i < maxBuffers; i++) {
            this.slots[i] = new BufferSlot();
        }
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Acquires a buffer with at least the requested capacity.
     *
     * <p>
     * Search strategy:
     * </p>
     * <ol>
     *   <li>Look for unused buffer with sufficient capacity</li>
     *   <li>If found, mark as in-use and return</li>
     *   <li>Otherwise, allocate new buffer (may not be cached on release if too large)</li>
     * </ol>
     *
     * @param requestedSize Minimum buffer capacity needed
     * @return A buffer with capacity &gt;= requestedSize
     */
    public CustomByteBuffer acquire(int requestedSize) {
        // Try to find an available buffer with sufficient capacity
        for (BufferSlot slot : slots) {
            if (slot.acquire(requestedSize)) {
                return slot.buffer;
            }
        }

        // No suitable buffer found, allocate new one
        return CustomByteBuffer.allocate(requestedSize);
    }

    /**
     * Returns a buffer to the pool.
     *
     * <p>
     * The buffer is marked as available for reuse. If the buffer is not from this pool
     * or exceeds {@code maxBufferSize}, it is ignored (will be GC'd).
     * </p>
     *
     * @param buffer The buffer to return (can be null)
     */
    public void release(CustomByteBuffer buffer) {
        if (buffer == null)
            return;

        // Find the buffer in our slots and mark as available
        for (BufferSlot slot : slots) {
            if (slot.release(buffer))
                return;
        }

        // Buffer not from this pool - check if we should cache it
        int capacity = buffer.capacity();
        if (capacity > maxBufferSize)
            return;

        // Try to cache in first available slot or with a smaller buffer
        for (BufferSlot slot : slots) {
            if (slot.tryReplace(buffer))
                return;
        }

        // Can't cache - buffer will be GC'd
    }

    /**
     * Clears all buffers from the pool.
     * Useful for cleanup when thread terminates.
     */
    public void clear() {
        for (BufferSlot slot : slots) {
            slot.buffer = null;
            slot.inUse = false;
        }
    }

    /**
     * Returns the number of buffers currently cached.
     */
    int cachedBufferCount() {
        int count = 0;
        for (BufferSlot slot : slots) {
            if (slot.buffer != null) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the number of buffers currently in use.
     */
    int inUseCount() {
        int count = 0;
        for (BufferSlot slot : slots) {
            if (slot.inUse) {
                count++;
            }
        }
        return count;
    }
}
