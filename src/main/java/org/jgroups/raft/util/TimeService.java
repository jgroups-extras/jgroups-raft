package org.jgroups.raft.util;

import java.time.Instant;

public interface TimeService {

    Instant now();

    long nanos();

    long interval(long start);

    static TimeService create(boolean enabled) {
        return enabled ? new SystemTimeService() : new DisabledTimeService();
    }

    final class DisabledTimeService implements TimeService {

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

    final class SystemTimeService implements TimeService {
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
