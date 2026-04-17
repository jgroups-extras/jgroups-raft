package org.jgroups.protocols.raft;

/**
 * Controls the log entry cache that sits between the Raft protocol and the underlying persistent log.
 *
 * <p>
 * When enabled, recently written entries are kept in memory so that followers can be caught up without reading from disk.
 * When disabled, the cache becomes a transparent passthrough: all reads and writes go directly to the underlying log.
 * </p>
 *
 * <p>
 * Obtain an instance via {@link Log#findCapability(Class) log.findCapability(LogCacheControl.class)}. Returns {@code null}
 * when the log stack does not include a caching layer.
 * </p>
 *
 * @since 2.0
 * @see Log#findCapability(Class)
 */
public interface LogCacheControl extends CacheCapability {

    /**
     * Enables caching with the given maximum number of entries.
     *
     * @param maxSize the maximum number of entries to retain in the cache
     */
    void enable(int maxSize);

    /**
     * Disables caching.
     *
     * <p>
     * The cache is cleared and all subsequent operations pass through to the underlying log.
     * </p>
     */
    void disable();

    /**
     * Evicts all cached entries without disabling the cache.
     */
    void clear();

    /**
     * Evicts the oldest entries until the cache size is within the configured maximum.
     */
    void trim();

    /**
     * Resets all cache statistics (accesses, hits, trims) to zero.
     */
    void resetStats();

    /**
     * Returns the maximum number of entries the cache will retain.
     *
     * @return the cache capacity
     */
    int maxSize();

    /**
     * Sets the maximum number of entries the cache will retain.
     *
     * @param size the new cache capacity
     */
    void maxSize(int size);

    /**
     * Returns the current number of entries in the cache.
     *
     * @return the number of cached entries
     */
    int cacheSize();

    /**
     * Returns the number of times the cache has been trimmed to stay within capacity.
     *
     * @return the trim count
     */
    int numTrims();

    /**
     * Returns the total number of cache lookups (hits + misses).
     *
     * @return the total access count
     */
    int numAccesses();

    /**
     * Returns the fraction of cache lookups that were served from the cache.
     *
     * @return the hit ratio, between 0.0 and 1.0, or 0.0 if no accesses have occurred
     */
    double hitRatio();

    /**
     * Returns a human-readable summary of the cache and its underlying log.
     *
     * @return a description string
     */
    String description();
}
