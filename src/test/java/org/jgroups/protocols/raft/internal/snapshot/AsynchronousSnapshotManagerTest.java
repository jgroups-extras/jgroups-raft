package org.jgroups.protocols.raft.internal.snapshot;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.protocols.raft.internal.RaftEventLoop;
import org.jgroups.raft.AsyncSnapshot;
import org.jgroups.raft.SnapshotHandle;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class AsynchronousSnapshotManagerTest {

    private static final String LOG_NAME = "asm-test-log";
    private static final List<String> MEMBERS = List.of("A", "B", "C");

    private InMemoryLog log;
    private PersistentState persistentState;
    private DefaultSnapshotMetrics metrics;
    private ExecutorService backgroundExecutor;
    private ExecutorService eventLoopExecutor;

    @BeforeMethod
    protected void setup() throws Exception {
        log = new InMemoryLog();
        log.init(LOG_NAME, null);
        persistentState = new PersistentState();
        persistentState.setMembers(MEMBERS);
        metrics = new DefaultSnapshotMetrics();
        backgroundExecutor = Executors.newSingleThreadExecutor();
        eventLoopExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        backgroundExecutor.shutdownNow();
        eventLoopExecutor.shutdownNow();
        backgroundExecutor.awaitTermination(5, SECONDS);
        eventLoopExecutor.awaitTermination(5, SECONDS);
        InMemoryLog.logs.remove(LOG_NAME);
    }

    public void testCreate() throws Exception {
        TestSnapshotHandle handle = new TestSnapshotHandle(42);
        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        asyncSnapshot.nextHandle = handle;

        AsynchronousSnapshotManager manager = createManager(asyncSnapshot, createEventLoop());

        AtomicBoolean actionCalled = new AtomicBoolean();
        manager.create(5, idx -> {
            assertThat(idx).isEqualTo(5);
            actionCalled.set(true);
        });

        assertThat(handle.awaitRelease(5, SECONDS)).isTrue();

        assertThat(actionCalled.get()).isTrue();
        assertThat(metrics.numSnapshots()).isEqualTo(1);

        ByteBuffer snapshot = log.getSnapshot();
        assertThat(snapshot).isNotNull();

        DataInput in = new ByteArrayDataInputStream(snapshot);
        PersistentState restored = new PersistentState();
        restored.readFrom(in);
        assertThat(restored.getMembers()).containsExactlyInAnyOrderElementsOf(MEMBERS);
        assertThat(in.readInt()).isEqualTo(42);
    }

    public void testCreateSkipsWhenInProgress() throws Exception {
        CountDownLatch writeGate = new CountDownLatch(1);
        TestSnapshotHandle handle = new TestSnapshotHandle(42, writeGate);
        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        asyncSnapshot.nextHandle = handle;

        AsynchronousSnapshotManager manager = createManager(asyncSnapshot, createEventLoop());

        // We call create for the first snapshot and let it idle asynchronously while writing.
        manager.create(5, idx -> {});

        assertThat(handle.awaitWriteStarted(5, SECONDS)).isTrue();

        // Submitting a second snapshot will return immediatelly, since it is still in progress.
        manager.create(10, idx -> {});

        // Release the first snapshot to complete.
        writeGate.countDown();
        assertThat(handle.awaitRelease(5, SECONDS)).isTrue();

        // Only one snapshot was taken
        assertThat(asyncSnapshot.prepareCallCount.get()).isEqualTo(1);
        assertThat(metrics.numSnapshots()).isEqualTo(1);

        // Submit another snapshot that should proceed normally
        TestSnapshotHandle secondHandle = new TestSnapshotHandle(99);
        asyncSnapshot.nextHandle = secondHandle;
        manager.create(15, idx -> {});
        assertThat(secondHandle.awaitRelease(5, SECONDS)).isTrue();

        assertThat(asyncSnapshot.prepareCallCount.get()).isEqualTo(2);
        assertThat(metrics.numSnapshots()).isEqualTo(2);
    }

    public void testCreatePrepareSnapshotFailure() {
        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        asyncSnapshot.prepareThrows = true;

        AsynchronousSnapshotManager manager = createManager(asyncSnapshot, createEventLoop());

        assertThatThrownBy(() -> manager.create(1, idx -> {}))
                .isInstanceOf(IOException.class);

        assertThat(metrics.numSnapshots()).isZero();
        assertThat(metrics.numFailedSnapshotsTaken()).isEqualTo(1);
    }

    public void testCreateWriteToFailure() throws Exception {
        TestSnapshotHandle handle = new TestSnapshotHandle(42);
        handle.writeThrows = true;
        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        asyncSnapshot.nextHandle = handle;

        AsynchronousSnapshotManager manager = createManager(asyncSnapshot, createEventLoop());

        manager.create(5, idx -> {});

        assertThat(handle.awaitRelease(5, SECONDS)).isTrue();

        assertThat(handle.released.get()).isTrue();
        assertThat(metrics.numSnapshots()).isZero();
        assertThat(metrics.numFailedSnapshotsTaken()).isEqualTo(1);
        assertThat(log.getSnapshot()).isNull();
    }

    public void testCreateExecutorRejection() {
        TestSnapshotHandle handle = new TestSnapshotHandle(42);
        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        asyncSnapshot.nextHandle = handle;

        ExecutorService rejected = Executors.newSingleThreadExecutor();
        rejected.shutdownNow();

        AsynchronousSnapshotManager manager = createManager(asyncSnapshot, createEventLoop(rejected));

        assertThatThrownBy(() -> manager.create(5, idx -> {}))
                .isInstanceOf(RejectedExecutionException.class);

        assertThat(handle.released.get()).isTrue();
        assertThat(metrics.numFailedSnapshotsTaken()).isEqualTo(1);
    }

    public void testInstall() throws Exception {
        ByteBuffer data = createSnapshotBuffer(persistentState, 42);

        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        PersistentState targetState = new PersistentState();
        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, targetState, log, noOpSender(), metrics);

        manager.install(data, 5, 2, (idx, term) -> {
            assertThat(idx).isEqualTo(5);
            assertThat(term).isEqualTo(2);
        });

        assertThat(target.counter).isEqualTo(42);
        assertThat(targetState.getMembers()).containsExactlyInAnyOrderElementsOf(MEMBERS);

        assertThat(log.firstAppended()).isEqualTo(5);
        assertThat(log.lastAppended()).isEqualTo(5);
        assertThat(log.commitIndex()).isEqualTo(5);

        assertThat(log.getSnapshot()).isNotNull();
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);
    }

    public void testInstallFailure() throws Exception {
        ByteBuffer data = createSnapshotBuffer(persistentState, 42);

        TestAsyncSnapshot failing = new TestAsyncSnapshot(0);
        failing.readThrows = true;
        AsynchronousSnapshotManager manager = createManager(failing, createEventLoop());

        AtomicBoolean actionCalled = new AtomicBoolean();
        assertThatThrownBy(() -> manager.install(data, 5, 2, (idx, term) -> actionCalled.set(true)))
                .isInstanceOf(RuntimeException.class);

        assertThat(actionCalled.get()).isFalse();
        assertThat(metrics.numSnapshotsReceived()).isZero();
        assertThat(metrics.numFailedSnapshotsInstalled()).isEqualTo(1);
    }

    public void testTransferTo() throws Exception {
        ByteBuffer snapshot = createSnapshotBuffer(persistentState, 42);
        log.setSnapshot(snapshot);

        Address dest = Util.createRandomAddress("target");
        AtomicBoolean senderCalled = new AtomicBoolean();

        SnapshotSender capturingSender = (d, sn, idx, term) -> {
            assertThat(d).isEqualTo(dest);
            assertThat(idx).isEqualTo(5);
            assertThat(term).isEqualTo(2);
            assertThat(sn).isNotNull();
            assertThat(sn.remaining()).isGreaterThan(0);
            senderCalled.set(true);
        };

        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), asyncSnapshot, persistentState, log, capturingSender, metrics);
        manager.transferTo(dest, 5, 2);

        assertThat(senderCalled.get()).isTrue();
    }

    public void testMetricsReset() throws Exception {
        TestSnapshotHandle handle = new TestSnapshotHandle(42);
        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        asyncSnapshot.nextHandle = handle;

        AsynchronousSnapshotManager manager = createManager(asyncSnapshot, createEventLoop());

        manager.create(1, idx -> {});
        assertThat(handle.awaitRelease(5, SECONDS)).isTrue();

        ByteBuffer data = createSnapshotBuffer(persistentState, 42);
        manager.install(data, 1, 1, (idx, term) -> {});

        assertThat(metrics.numSnapshots()).isEqualTo(1);
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);

        metrics.reset();

        assertThat(metrics.numSnapshots()).isZero();
        assertThat(metrics.numSnapshotsReceived()).isZero();
        assertThat(metrics.numFailedSnapshotsTaken()).isZero();
        assertThat(metrics.numFailedSnapshotsInstalled()).isZero();
    }

    private AsynchronousSnapshotManager createManager(TestAsyncSnapshot asyncSnapshot, RaftEventLoop eventLoop) {
        return new AsynchronousSnapshotManager(eventLoop, asyncSnapshot, persistentState, log, noOpSender(), metrics);
    }

    private RaftEventLoop createEventLoop() {
        return createEventLoop(backgroundExecutor);
    }

    private RaftEventLoop createEventLoop(Executor background) {
        return new RaftEventLoop() {
            @Override
            public <T> CompletionStage<T> submit(Callable<T> callable) {
                CompletableFuture<T> future = new CompletableFuture<>();
                eventLoopExecutor.execute(() -> {
                    try {
                        future.complete(callable.call());
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
                return future;
            }

            @Override
            public Executor executor() {
                return background;
            }
        };
    }

    private static SnapshotSender noOpSender() {
        return (dest, snapshot, lastIndex, lastTerm) -> {};
    }

    private static ByteBuffer createSnapshotBuffer(PersistentState ps, int value) throws Exception {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(256, true);
        ps.writeTo(out);
        out.writeInt(value);
        return ByteBuffer.wrap(out.buffer(), 0, out.position());
    }

    private static class TestAsyncSnapshot implements AsyncSnapshot {
        int counter;
        volatile SnapshotHandle nextHandle;
        volatile boolean prepareThrows;
        volatile boolean readThrows;
        final AtomicInteger prepareCallCount = new AtomicInteger();

        TestAsyncSnapshot(int initial) {
            this.counter = initial;
        }

        @Override
        public SnapshotHandle prepareSnapshot() throws Exception {
            prepareCallCount.incrementAndGet();
            if (prepareThrows) throw new IOException("simulated prepare failure");
            return nextHandle;
        }

        @Override
        public void readContentFrom(DataInput in) {
            if (readThrows) throw new RuntimeException("simulated read failure");
            try {
                counter = in.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class TestSnapshotHandle implements SnapshotHandle {
        private final int value;
        private final CountDownLatch writeGate;
        private final CountDownLatch writeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseLatch = new CountDownLatch(1);
        final AtomicBoolean released = new AtomicBoolean();
        volatile boolean writeThrows;

        TestSnapshotHandle(int value) {
            this(value, new CountDownLatch(0));
        }

        TestSnapshotHandle(int value, CountDownLatch writeGate) {
            this.value = value;
            this.writeGate = writeGate;
        }

        @Override
        public void writeTo(DataOutput out) throws Exception {
            writeStarted.countDown();
            writeGate.await();
            if (writeThrows) throw new IOException("simulated write failure");
            out.writeInt(value);
        }

        @Override
        public void release() {
            released.set(true);
            releaseLatch.countDown();
        }

        boolean awaitRelease(long timeout, TimeUnit unit) throws InterruptedException {
            return releaseLatch.await(timeout, unit);
        }

        boolean awaitWriteStarted(long timeout, TimeUnit unit) throws InterruptedException {
            return writeStarted.await(timeout, unit);
        }
    }
}
