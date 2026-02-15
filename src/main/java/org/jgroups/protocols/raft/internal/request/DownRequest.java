package org.jgroups.protocols.raft.internal.request;

import org.jgroups.raft.Options;
import org.jgroups.raft.internal.metrics.SystemMetricsTracker;
import org.jgroups.raft.util.TimeService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract sealed class DownRequest extends CompletableFuture<byte[]> implements BaseRequest {
    private final CompletableFuture<byte[]> future;
    private final byte[] buffer;
    private final int offset;
    private final int length;
    private final boolean internal;
    private final Options options;
    private final boolean readOnly;

    protected DownRequest(CompletableFuture<byte[]> future, byte[] buffer, int offset, int length, boolean internal, Options options, boolean readOnly) {
        this.future = future;
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        this.internal = internal;
        this.options = options;
        this.readOnly = readOnly;
    }

    public Options options() {
        return options;
    }

    public byte[] buffer() {
        return buffer;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public boolean isInternal() {
        return internal;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public final boolean complete(byte[] result) {
        completeTrackingOperations();
        super.complete(result);
        return future.complete(result);
    }

    @Override
    public final boolean completeExceptionally(Throwable ex) {
        return failed(ex);
    }

    @Override
    public final boolean failed(Throwable t) {
        completeTrackingOperations();
        super.completeExceptionally(t);
        return future.completeExceptionally(t);
    }

    private void completeTrackingOperations() {
        completeUserOperation();
        completeReplication();
    }

    static DownRequest create(CompletableFuture<byte[]> future, byte[] buffer, int offset, int length, boolean internal, Options options,
                              boolean readOnly, SystemMetricsTracker tracker, TimeService timeService) {
        if (tracker == null || timeService == null) {
            return new Untracked(future, buffer, offset, length, internal, options, readOnly);
        }
        return new Tracked(future, buffer, offset, length, internal, options, readOnly, tracker, timeService);
    }

    private static final class Untracked extends DownRequest {

        Untracked(CompletableFuture<byte[]> future, byte[] buffer, int offset, int length, boolean internal, Options options, boolean readOnly) {
            super(future, buffer, offset, length, internal, options, readOnly);
        }

        @Override
        public void startUserOperation() { }

        @Override
        public void completeUserOperation() { }

        @Override
        public void startReplication() { }

        @Override
        public void completeReplication() { }
    }

    private static final class Tracked extends DownRequest {
        private static final AtomicLongFieldUpdater<Tracked> USER_START_NANOS_UPDATER = AtomicLongFieldUpdater.newUpdater(Tracked.class, "userStartNanos");
        private static final AtomicLongFieldUpdater<Tracked> PROCESSING_START_NANOS_UPDATER = AtomicLongFieldUpdater.newUpdater(Tracked.class, "processingStartNanos");

        private final SystemMetricsTracker tracker;
        private final TimeService timeService;

        private volatile long userStartNanos;
        private volatile long processingStartNanos;

        Tracked(CompletableFuture<byte[]> future, byte[] buffer, int offset, int length, boolean internal, Options options,
                boolean readOnly, SystemMetricsTracker tracker, TimeService timeService) {
            super(future, buffer, offset, length, internal, options, readOnly);
            this.tracker = tracker;
            this.timeService = timeService;
        }

        @Override
        public void startUserOperation() {
            USER_START_NANOS_UPDATER.set(this, timeService.nanos());
        }

        @Override
        public void completeUserOperation() {
            long diff = timeService.interval(USER_START_NANOS_UPDATER.get(this));
            tracker.recordCommandProcessingLatency(diff);
        }

        @Override
        public void startReplication() {
            PROCESSING_START_NANOS_UPDATER.set(this, timeService.nanos());
        }

        @Override
        public void completeReplication() {
            long diff = timeService.interval(PROCESSING_START_NANOS_UPDATER.get(this));
            tracker.recordReplicationLatency(diff);
        }
    }
}
