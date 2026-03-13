package org.jgroups.raft.util;

import java.time.Instant;

/**
 * Abstraction over time sources.
 *
 * <p>
 * Direct calls to {@link System#nanoTime()} and {@link Instant#now()} scatter through the codebase and make unit tests
 * dependent on real time. {@code TimeService} centralizes all time reads behind a single interface so that:
 * </p>
 *
 * <ul>
 *     <li>Tests can supply a controlled implementation with deterministic timestamps.</li>
 *     <li>Metrics collection can be disabled at the source</li>
 * </ul>
 *
 * <h2>Wall-Clock vs Monotonic Time</h2>
 *
 * <p>
 * The interface provides both kinds of time:
 * </p>
 *
 * <ul>
 *     <li>{@link #now()}: wall-clock {@link Instant}, suitable for human-readable timestamps.</li>
 *     <li>{@link #nanos()} and {@link #interval(long)}: monotonic nanoseconds, suitable for latency measurement or timeouts.
 *         Monotonic time is immune to clock adjustments and NTP drift.</li>
 * </ul>
 *
 * <h2>Obtaining an Instance</h2>
 *
 * <p>
 * Use the factory method {@link TimeService#create(boolean)}. Pass {@code true} to get a live time source backed by the
 * system time source, or {@code false} to get a no-op implementation that returns zero/null for all methods.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public interface TimeService {

    /**
     * Returns the current wall-clock time.
     *
     * <p>
     * Use this for human-readable timestamps. Do not use for latency measurement, wall-clock time is subject to NTP adjustments.
     * </p>
     *
     * @return the current instant, or {@code null} when the service is disabled
     */
    Instant now();

    /**
     * Returns a monotonic nanosecond timestamp.
     *
     * <p>
     * Capture a value with {@code nanos()} at the start of an operation, then pass it to {@link #interval(long)} to compute
     * the elapsed time. The absolute value has no meaning outside of interval computation.
     * </p>
     *
     * @return a monotonic nanosecond value, or {@code 0} when disabled
     */
    long nanos();

    /**
     * Computes the elapsed nanoseconds since a previously captured {@link #nanos()} value.
     *
     * <p>
     * Typical usage:
     * </p>
     *
     * <pre>{@code
     * long start = timeService.nanos();
     * // ... operation ...
     * long elapsedNanos = timeService.interval(start);
     * }</pre>
     *
     * @param start a value previously returned by {@link #nanos()}
     * @return elapsed nanoseconds, or {@code 0} when disabled
     */
    long interval(long start);

    /**
     * Creates a {@link TimeService} instance.
     *
     * @param enabled {@code true} for a live time source, {@code false} for a no-op implementation
     * @return a time service backed by system clocks, or a disabled stub that returns zero/null
     */
    static TimeService create(boolean enabled) {
        return enabled ? new SystemTimeService() : new DisabledTimeService();
    }

    /**
     * No-op implementation that returns {@code null} for timestamps and {@code 0} for
     * nanosecond values. Used when metrics collection is disabled.
     */
    final class DisabledTimeService implements TimeService {

        private DisabledTimeService() { }

        @Override
        public Instant now() {
            return null;
        }

        @Override
        public long nanos() {
            return 0;
        }

        @Override
        public long interval(long start) {
            return 0;
        }
    }

    /**
     * Live implementation backed by the JVM time.
     */
    final class SystemTimeService implements TimeService {

        private SystemTimeService() { }

        @Override
        public Instant now() {
            return Instant.now();
        }

        public long nanos() {
            return System.nanoTime();
        }

        public long interval(long start) {
            return nanos() - start;
        }
    }
}
