package org.jgroups.raft.tests.harness;

import org.jgroups.raft.util.TimeService;

import java.time.Instant;

public class ControlledTimeService implements TimeService {
    private long time;

    public void advance(long offset) {
        time += offset;
    }

    @Override
    public Instant now() {
        return Instant.ofEpochMilli(time / 1000);
    }

    @Override
    public long nanos() {
        return time;
    }

    @Override
    public long interval(long start) {
        return time - start;
    }
}
