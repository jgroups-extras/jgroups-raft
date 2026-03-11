package org.jgroups.protocols.raft.internal.request;

import org.jgroups.raft.Options;
import org.jgroups.raft.internal.metrics.RaftProtocolMetrics;
import org.jgroups.raft.util.TimeService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A user-submitted request flowing down the protocol stack.
 *
 * <p>
 * Carries the request data and a {@link CompletableFuture} that resolves when the
 * operation completes or fails. When metrics are enabled, latency is recorded for
 * both the user operation and replication spans; otherwise, tracking is a no-op.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public abstract sealed class DownRequest extends CompletableFuture<byte[]> implements BaseRequest permits DownRequest.Tracked, DownRequest.Untracked {
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

    /**
     * @return the options associated with the request, or {@code null} if none.
     */
    public Options options() {
        return options;
    }

    /**
     * @return the serialized request payload.
     */
    public byte[] buffer() {
        return buffer;
    }

    /**
     * @return the starting offset within the {@link #buffer()}.
     */
    public int offset() {
        return offset;
    }

    /**
     * @return the number of bytes to read from {@link #buffer()} starting at {@link #offset()}.
     */
    public int length() {
        return length;
    }

    /**
     * @return {@code true} if this is an internal protocol operation (e.g., membership change).
     */
    public boolean isInternal() {
        return internal;
    }

    /**
     * @return {@code true} if this is a read-only request that does not modify the log.
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Completes the request successfully, recording the user operation latency
     * and resolving the user-facing future.
     */
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

    /**
     * Fails the request, recording the user operation latency and completing
     * the user-facing future exceptionally.
     */
    @Override
    public final boolean failed(Throwable t) {
        completeTrackingOperations();
        super.completeExceptionally(t);
        return future.completeExceptionally(t);
    }

    private void completeTrackingOperations() {
        completeUserOperation();
    }

    /**
     * Creates a {@link DownRequest} with or without latency tracking depending on whether
     * metrics and time service are available.
     */
    static DownRequest create(CompletableFuture<byte[]> future, byte[] buffer, int offset, int length, boolean internal, Options options,
                              boolean readOnly, RaftProtocolMetrics metrics, TimeService timeService) {
        if (metrics == null || timeService == null) {
            return new Untracked(future, buffer, offset, length, internal, options, readOnly);
        }
        return new Tracked(future, buffer, offset, length, internal, options, readOnly, metrics, timeService);
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

    @SuppressWarnings("PMD.UnusedPrivateField")
    private static final class Tracked extends DownRequest {
        private static final AtomicLongFieldUpdater<Tracked> USER_START_NANOS_UPDATER = AtomicLongFieldUpdater.newUpdater(Tracked.class, "userStartNanos");
        private static final AtomicLongFieldUpdater<Tracked> PROCESSING_START_NANOS_UPDATER = AtomicLongFieldUpdater.newUpdater(Tracked.class, "processingStartNanos");

        private final RaftProtocolMetrics metrics;
        private final TimeService timeService;

        private volatile long userStartNanos;
        private volatile long processingStartNanos;

        Tracked(CompletableFuture<byte[]> future, byte[] buffer, int offset, int length, boolean internal, Options options,
                boolean readOnly, RaftProtocolMetrics metrics, TimeService timeService) {
            super(future, buffer, offset, length, internal, options, readOnly);
            this.metrics = metrics;
            this.timeService = timeService;
        }

        @Override
        public void startUserOperation() {
            USER_START_NANOS_UPDATER.set(this, timeService.nanos());
        }

        @Override
        public void completeUserOperation() {
            long diff = timeService.interval(USER_START_NANOS_UPDATER.get(this));
            metrics.recordTotalLatency(diff);
        }

        @Override
        public void startReplication() {
            PROCESSING_START_NANOS_UPDATER.set(this, timeService.nanos());
        }

        @Override
        public void completeReplication() {
            long diff = timeService.interval(PROCESSING_START_NANOS_UPDATER.get(this));
            metrics.recordProcessingLatency(diff);
        }
    }
}
