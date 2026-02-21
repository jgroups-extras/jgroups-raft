package org.jgroups.raft.internal.serialization.binary;

/**
 * A lightweight cache that learns optimal buffer sizes from actual serialization operations.
 *
 * <p>
 * This cache eliminates the need to pre-calculate serialized sizes by learning from usage patterns. It tracks the serialized
 * size for each object type and uses this information to allocate appropriately sized buffers for future serializations,
 * reducing buffer resizing overhead.
 * </p>
 *
 * <h2>Design Philosophy</h2>
 *
 * <p>
 * Rather than traversing object graphs twice (once to calculate size, once to serialize), this cache takes a pragmatic approach:
 * serialize once, remember the size, use it next time. This is faster for complex objects and simpler to implement.
 * </p>
 *
 * <h2>Adaptive Sizing Strategy</h2>
 *
 * <p>
 * The cache uses asymmetric update logic to balance responsiveness with stability:
 * </p>
 * <ul>
 *   <li><b>Eager increase:</b> If actual size exceeds cached size, immediately update to {@code actualSize + 10%} to
 *                              reduce future buffer resizes</li>
 *   <li><b>Slow decrease:</b> If actual size is smaller, gradually converge using weighted average
 *                             ({@code 90% old + 10% new}) to avoid thrashing from temporary variations</li>
 * </ul>
 *
 * <h2>Example Behavior</h2>
 *
 * <pre>
 * Serialize RaftCommand (first time):
 *   Cache miss → use default buffer size defined in {@link BinarySerializer}
 *   Actual size: 850 bytes
 *   Cache stores: 850 + 85 = 935 bytes
 *
 * Serialize RaftCommand (second time, larger payload):
 *   Cache hit → use 935 bytes
 *   Actual size: 1200 bytes (exceeds cached)
 *   Cache updates: 1200 + 120 = 1320 bytes (eager increase)
 *
 * Serialize RaftCommand (third time, smaller payload):
 *   Cache hit → use 1320 bytes
 *   Actual size: 900 bytes (under cached)
 *   Cache updates: (1320 x 0.9) + (900 x 0.1) = 1278 bytes (slow decrease)
 * </pre>
 *
 * <h2>Cache Structure</h2>
 *
 * <p>
 * Uses parallel arrays with linear search for simplicity and cache locality:
 * </p>
 * <ul>
 *   <li><b>Size:</b> {@link SerializationSizeCache#CACHE_SIZE} entries</li>
 *   <li><b>Lookup:</b> Linear scan with {@link Class} identity comparison</li>
 *   <li><b>Eviction:</b> Round-robin replacement (no LRU tracking overhead)</li>
 *   <li><b>Memory:</b> 8 x (reference + int) ≈ 96 bytes per instance</li>
 * </ul>
 *
 * <p>
 * Linear search of 8 entries is faster than HashMap for small N due to:
 * </p>
 * <ul>
 *   <li>No hash computation</li>
 *   <li>No bucket traversal</li>
 *   <li>Better CPU cache locality</li>
 *   <li>Predictable memory access pattern</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 * This class is <b>not thread-safe</b>. It is designed to be used with {@link ThreadLocal} to provide one cache instance
 * per thread, eliminating contention:
 * </p>
 *
 * <pre>{@code
 * private static final ThreadLocal<SerializationSizeCache> CACHE =
 *     ThreadLocal.withInitial(SerializationSizeCache::new);
 * }</pre>
 *
 * <h2>Limitations</h2>
 *
 * <ul>
 *   <li>Only tracks by {@link Class}, not by actual object content</li>
 *   <li>Size estimates may be suboptimal for highly variable payloads</li>
 * </ul>
 *
 * @author José Bolina
 * @since 2.0
 * @see BinarySerializer
 */
final class SerializationSizeCache {

    private static final int CACHE_SIZE = 8;

    /**
     * Padding percentage added to cached sizes.
     *
     * <p>
     * When updating the cache, we add 10% padding to reduce the probability of future buffer resizes.
     * </p>
     */
    private static final int PADDING_PERCENT = 10;

    /**
     * Parallel arrays for cache entries (faster than object allocation).
     */
    private final Class<?>[] types = new Class<?>[CACHE_SIZE];
    private final int[] sizes = new int[CACHE_SIZE];

    /**
     * Next slot for round-robin eviction.
     */
    private int nextSlot = 0;

    /**
     * Looks up the cached size for the given type.
     *
     * @param type The object type
     * @return The cached size in bytes, or -1 if not found
     */
    public int lookup(Class<?> type) {
        // Linear search is fine for 8 entries (likely faster than hash due to locality)
        for (int i = 0; i < CACHE_SIZE; i++) {
            if (types[i] == type) {  // Identity comparison (same Class object)
                return sizes[i];
            }
        }
        return -1;
    }

    /**
     * Updates the cache with the actual serialized size.
     *
     * <p>
     * This method implements adaptive sizing:
     * <ul>
     *   <li>If the actual size is larger than cached, eagerly increase with padding</li>
     *   <li>If the actual size is smaller, slowly decrease to avoid thrashing</li>
     * </ul>
     * </p>
     *
     * @param type The object type
     * @param actualSize The actual serialized size in bytes
     */
    public void update(Class<?> type, int actualSize) {
        // Try to find and update existing entry
        for (int i = 0; i < CACHE_SIZE; i++) {
            if (types[i] == type) {
                if (actualSize > sizes[i]) {
                    // Eager increase: set to actual size + padding to reduce future resizes
                    sizes[i] = actualSize + (actualSize / PADDING_PERCENT);
                } else if (actualSize < sizes[i]) {
                    // Slow decrease: weighted average (90% old, 10% new)
                    // This prevents thrashing from temporary size variations
                    sizes[i] = (sizes[i] * 9 + actualSize) / 10;
                }
                // else: actualSize == sizes[i], no update needed
                return;
            }
        }

        // Not found: insert new entry with round-robin eviction
        types[nextSlot] = type;
        sizes[nextSlot] = actualSize + (actualSize / PADDING_PERCENT);
        nextSlot = (nextSlot + 1) % CACHE_SIZE;
    }
}
