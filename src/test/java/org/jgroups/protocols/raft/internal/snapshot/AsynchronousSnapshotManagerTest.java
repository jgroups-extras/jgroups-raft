package org.jgroups.protocols.raft.internal.snapshot;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.protocols.raft.internal.RaftEventLoop;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkRequest;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotChunkResponse;
import org.jgroups.protocols.raft.internal.snapshot.messages.SnapshotMetadataRequest;
import org.jgroups.raft.AsyncSnapshot;
import org.jgroups.raft.SnapshotHandle;
import org.jgroups.raft.util.TimeService;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
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
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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
    private Path tempDir;

    @BeforeMethod
    protected void setup() throws Exception {
        log = new InMemoryLog();
        log.init(LOG_NAME, null);
        persistentState = new PersistentState();
        persistentState.setMembers(MEMBERS);
        metrics = new DefaultSnapshotMetrics(TimeService.create(false));
        backgroundExecutor = Executors.newSingleThreadExecutor();
        eventLoopExecutor = Executors.newSingleThreadExecutor();
        tempDir = Files.createTempDirectory("asm-test");
    }

    @AfterMethod
    protected void cleanup() throws Exception {
        backgroundExecutor.shutdownNow();
        eventLoopExecutor.shutdownNow();
        backgroundExecutor.awaitTermination(5, SECONDS);
        eventLoopExecutor.awaitTermination(5, SECONDS);
        InMemoryLog.logs.remove(LOG_NAME);
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
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

    public void testChunkedInstall() throws Exception {
        ByteBuffer snapshotData = createSnapshotBuffer(persistentState, 42);
        byte[] raw = new byte[snapshotData.remaining()];
        snapshotData.get(raw);

        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        PersistentState targetState = new PersistentState();
        int testChunkSize = 16;

        CapturingSnapshotSender capturingSender = new CapturingSnapshotSender();
        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, targetState, log, capturingSender, metrics,
                tempDir, testChunkSize, 16);

        long term = 1;
        long lastIndex = 5;
        long lastTerm = 2;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, raw.length), (idx, t) -> {});

        assertThat(capturingSender.chunkRequestCount.get()).isEqualTo(1);

        int totalChunks = (int) Math.ceil((double) raw.length / testChunkSize);
        for (int i = 0; i < totalChunks; i++) {
            int offset = i * testChunkSize;
            int len = Math.min(testChunkSize, raw.length - offset);
            boolean last = (i == totalChunks - 1);

            ByteBuffer chunk = ByteBuffer.wrap(raw, offset, len);
            manager.install(chunk,
                    new SnapshotChunkResponse(term, lastIndex, offset, last), (idx, t) -> {});
        }

        assertThat(target.counter).isEqualTo(42);
        assertThat(targetState.getMembers()).containsExactlyInAnyOrderElementsOf(MEMBERS);

        assertThat(log.firstAppended()).isEqualTo(lastIndex);
        assertThat(log.lastAppended()).isEqualTo(lastIndex);
        assertThat(log.commitIndex()).isEqualTo(lastIndex);

        assertThat(log.getSnapshot()).isNotNull();
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);
        assertThat(metrics.numChunksReceived()).isEqualTo(totalChunks);
        assertThat(metrics.numBytesReceived()).isEqualTo(raw.length);
    }

    public void testChunkedInstallSingleChunk() throws Exception {
        ByteBuffer snapshotData = createSnapshotBuffer(persistentState, 99);
        byte[] raw = new byte[snapshotData.remaining()];
        snapshotData.get(raw);

        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        PersistentState targetState = new PersistentState();

        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, targetState, log, noOpSender(), metrics,
                tempDir, raw.length + 1, 1);

        long term = 1;
        long lastIndex = 10;
        long lastTerm = 3;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, raw.length),
                (idx, t) -> {});

        manager.install(ByteBuffer.wrap(raw),
                new SnapshotChunkResponse(term, lastIndex, 0, true),
                (idx, t) -> {
                    assertThat(idx).isEqualTo(lastIndex);
                    assertThat(t).isEqualTo(lastTerm);
                });

        assertThat(target.counter).isEqualTo(99);
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);
    }

    public void testChunkedInstallFailure() throws Exception {
        ByteBuffer snapshotData = createSnapshotBuffer(persistentState, 42);
        byte[] raw = new byte[snapshotData.remaining()];
        snapshotData.get(raw);

        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot failing = new TestAsyncSnapshot(0);
        failing.readThrows = true;

        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), failing, persistentState, log, noOpSender(), metrics,
                tempDir, raw.length + 1, 1);

        long term = 1;
        long lastIndex = 5;
        long lastTerm = 2;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, raw.length),
                (idx, t) -> {});

        assertThatThrownBy(() -> manager.install(ByteBuffer.wrap(raw),
                new SnapshotChunkResponse(term, lastIndex, 0, true),
                (idx, t) -> {}))
                .isInstanceOf(RuntimeException.class);

        assertThat(metrics.numSnapshotsReceived()).isZero();
        assertThat(metrics.numFailedSnapshotsInstalled()).isEqualTo(1);
        assertThat(metrics.numFailedChunkTransfers()).isEqualTo(1);
    }

    public void testDuplicateMetadataIgnored() throws Exception {
        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        CapturingSnapshotSender capturingSender = new CapturingSnapshotSender();

        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, persistentState, log, capturingSender, metrics,
                tempDir, 64, 4);

        long term = 1;
        long lastIndex = 5;
        long lastTerm = 2;
        long totalSize = 128;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, totalSize),
                (idx, t) -> {});

        assertThat(capturingSender.chunkRequestCount.get()).isEqualTo(1);

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, totalSize),
                (idx, t) -> {});

        assertThat(capturingSender.chunkRequestCount.get()).isEqualTo(1);
    }

    public void testDifferentMetadataAbortsAndRestarts() throws Exception {
        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        CapturingSnapshotSender capturingSender = new CapturingSnapshotSender();

        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, persistentState, log, capturingSender, metrics,
                tempDir, 64, 4);

        long term = 1;
        long lastTerm = 2;
        long totalSize = 128;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, 5, totalSize),
                (idx, t) -> {});

        assertThat(capturingSender.chunkRequestCount.get()).isEqualTo(1);

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, 10, totalSize),
                (idx, t) -> {});

        assertThat(capturingSender.chunkRequestCount.get()).isEqualTo(2);
        assertThat(metrics.numFailedChunkTransfers()).isEqualTo(1);
    }

    public void testChunkWithNoActiveTransferIgnored() throws Exception {
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        AsynchronousSnapshotManager manager = createManager(target, createEventLoop());

        manager.install(ByteBuffer.wrap(new byte[]{1, 2, 3}),
                new SnapshotChunkResponse(1, 5, 0, false),
                (idx, t) -> {});

        assertThat(metrics.numChunksReceived()).isZero();
    }

    public void testStaleChunkFromPreviousTransferIgnored() throws Exception {
        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);

        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, persistentState, log, noOpSender(), metrics,
                tempDir, 64, 4);

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, 1, 2, 10, 128),
                (idx, t) -> {});

        manager.install(ByteBuffer.wrap(new byte[]{1, 2, 3}),
                new SnapshotChunkResponse(1, 5, 0, false),
                (idx, t) -> {});

        assertThat(metrics.numChunksReceived()).isZero();
    }

    public void testOutOfOrderChunks() throws Exception {
        ByteBuffer snapshotData = createSnapshotBuffer(persistentState, 77);
        byte[] raw = new byte[snapshotData.remaining()];
        snapshotData.get(raw);

        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        PersistentState targetState = new PersistentState();
        int testChunkSize = 16;

        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, targetState, log, noOpSender(), metrics,
                tempDir, testChunkSize, 16);

        long term = 1;
        long lastIndex = 5;
        long lastTerm = 2;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, raw.length),
                (idx, t) -> {});

        int totalChunks = (int) Math.ceil((double) raw.length / testChunkSize);
        int[] order = new int[totalChunks];
        for (int i = 0; i < totalChunks; i++) order[i] = i;
        // Reverse the order
        for (int i = 0; i < totalChunks / 2; i++) {
            int tmp = order[i];
            order[i] = order[totalChunks - 1 - i];
            order[totalChunks - 1 - i] = tmp;
        }

        for (int idx : order) {
            int offset = idx * testChunkSize;
            int len = Math.min(testChunkSize, raw.length - offset);
            boolean last = (idx == totalChunks - 1);

            ByteBuffer chunk = ByteBuffer.wrap(raw, offset, len);
            manager.install(chunk,
                    new SnapshotChunkResponse(term, lastIndex, offset, last),
                    (i, t) -> {});
        }

        assertThat(target.counter).isEqualTo(77);
        assertThat(targetState.getMembers()).containsExactlyInAnyOrderElementsOf(MEMBERS);
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);
        assertThat(metrics.numChunksReceived()).isEqualTo(totalChunks);
    }

    public void testTransferToSendsMetadata() throws Exception {
        ByteBuffer snapshot = createSnapshotBuffer(persistentState, 42);
        log.setSnapshot(snapshot);
        log.currentTerm(3);

        Address dest = Util.createRandomAddress("target");
        long expectedSize = log.snapshotSize();
        AtomicBoolean metadataSent = new AtomicBoolean();

        SnapshotSender capturingSender = new NoOpSnapshotSender() {
            @Override
            public void sendMetadata(Address d, long currTerm, long lastIncludedIndex, long lastIncludedTerm, long totalSize) {
                assertThat(d).isEqualTo(dest);
                assertThat(currTerm).isEqualTo(3);
                assertThat(lastIncludedIndex).isEqualTo(5);
                assertThat(lastIncludedTerm).isEqualTo(2);
                assertThat(totalSize).isEqualTo(expectedSize);
                metadataSent.set(true);
            }
        };

        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), asyncSnapshot, persistentState, log, capturingSender, metrics,
                tempDir, 256 * 1024, 16);
        manager.transferTo(null, null, 5, 2, dest);

        assertThat(metadataSent.get()).isTrue();
    }

    public void testTransferToServesChunks() throws Exception {
        ByteBuffer snapshot = createSnapshotBuffer(persistentState, 42);
        byte[] expected = new byte[snapshot.remaining()];
        snapshot.duplicate().get(expected);
        log.setSnapshot(snapshot);
        log.currentTerm(3);

        Address dest = Util.createRandomAddress("target");
        int testChunkSize = 16;
        int totalChunks = (int) Math.ceil((double) expected.length / testChunkSize);

        CountDownLatch sendLatch = new CountDownLatch(totalChunks);
        byte[] reassembled = new byte[expected.length];
        AtomicBoolean lastChunkDone = new AtomicBoolean();

        SnapshotSender capturingSender = new NoOpSnapshotSender() {
            @Override
            public void sendChunkResponse(Address d, long currTerm, long lastIncludedIndex, ByteBuffer chunk, long offset, boolean done) {
                assertThat(d).isEqualTo(dest);
                assertThat(currTerm).isEqualTo(3);
                assertThat(lastIncludedIndex).isEqualTo(5);
                chunk.get(reassembled, (int) offset, chunk.remaining());
                if (done) lastChunkDone.set(true);
                sendLatch.countDown();
            }
        };

        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), asyncSnapshot, persistentState, log, capturingSender, metrics,
                tempDir, testChunkSize, totalChunks);

        SnapshotChunkRequest scr = new SnapshotChunkRequest(3, 5, 0, totalChunks);
        manager.transferTo(null, scr, 0, 0, dest);

        assertThat(sendLatch.await(5, SECONDS)).isTrue();
        assertThat(reassembled).isEqualTo(expected);
        assertThat(lastChunkDone.get()).isTrue();
    }

    public void testTransferToRequestBeyondSnapshotSize() throws Exception {
        ByteBuffer snapshot = createSnapshotBuffer(persistentState, 42);
        log.setSnapshot(snapshot);
        log.currentTerm(3);

        Address dest = Util.createRandomAddress("target");
        int testChunkSize = 16;
        long snapshotSize = log.snapshotSize();
        int actualChunks = (int) Math.ceil((double) snapshotSize / testChunkSize);
        int requestedChunks = actualChunks + 5;

        CountDownLatch sendLatch = new CountDownLatch(actualChunks);
        AtomicInteger chunkCount = new AtomicInteger();

        SnapshotSender capturingSender = new NoOpSnapshotSender() {
            @Override
            public void sendChunkResponse(Address d, long currTerm, long lastIncludedIndex, ByteBuffer chunk, long offset, boolean done) {
                chunkCount.incrementAndGet();
                sendLatch.countDown();
            }
        };

        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), asyncSnapshot, persistentState, log, capturingSender, metrics,
                tempDir, testChunkSize, requestedChunks);

        SnapshotChunkRequest scr = new SnapshotChunkRequest(3, 5, 0, requestedChunks);
        manager.transferTo(null, scr, 0, 0, dest);

        assertThat(sendLatch.await(5, SECONDS)).isTrue();
        assertThat(chunkCount.get()).isEqualTo(actualChunks);
    }

    public void testMetricsReset() throws Exception {
        TestSnapshotHandle handle = new TestSnapshotHandle(42);
        TestAsyncSnapshot asyncSnapshot = new TestAsyncSnapshot(0);
        asyncSnapshot.nextHandle = handle;

        AsynchronousSnapshotManager manager = createManager(asyncSnapshot, createEventLoop());

        manager.create(1, idx -> {});
        assertThat(handle.awaitRelease(5, SECONDS)).isTrue();

        assertThat(metrics.numSnapshots()).isEqualTo(1);

        metrics.reset();

        assertThat(metrics.numSnapshots()).isZero();
        assertThat(metrics.numSnapshotsReceived()).isZero();
        assertThat(metrics.numFailedSnapshotsTaken()).isZero();
        assertThat(metrics.numFailedSnapshotsInstalled()).isZero();
        assertThat(metrics.activeTransferTotalChunks()).isZero();
        assertThat(metrics.activeTransferChunksReceived()).isZero();
        assertThat(metrics.activeTransferChunksInFlight()).isZero();
        assertThat(metrics.activeTransferHighestRequested()).isZero();
        assertThat(metrics.activeTransferMissingChunks()).isEmpty();
    }

    @DataProvider
    public Object[][] slidingWindowConfigurations() {
        return new Object[][] {
                // chunkSize, batchSize
                { 16, 4 },
                { 16, 8 },
                { 13, 3 },
                { 37, 5 },
                { 17, 7 },
        };
    }

    @Test(dataProvider = "slidingWindowConfigurations")
    public void testSlidingWindowRefill(int testChunkSize, int testBatchSize) throws Exception {
        byte[] raw = createPaddedSnapshot(persistentState, 42, testChunkSize * 12);

        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        PersistentState targetState = new PersistentState();

        CapturingSnapshotSender capturingSender = new CapturingSnapshotSender();
        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, targetState, log, capturingSender, metrics,
                tempDir, testChunkSize, testBatchSize);

        long term = 1;
        long lastIndex = 5;
        long lastTerm = 2;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, raw.length), (idx, t) -> {});

        assertThat(capturingSender.chunkRequestCount.get()).isEqualTo(1);

        int totalChunks = (int) Math.ceil((double) raw.length / testChunkSize);
        for (int i = 0; i < totalChunks; i++) {
            int offset = i * testChunkSize;
            int len = Math.min(testChunkSize, raw.length - offset);
            boolean last = (i == totalChunks - 1);

            ByteBuffer chunk = ByteBuffer.wrap(raw, offset, len);
            manager.install(chunk,
                    new SnapshotChunkResponse(term, lastIndex, offset, last), (idx, t) -> {});
        }

        assertThat(capturingSender.chunkRequestCount.get()).isGreaterThan(1);
        assertThat(target.counter).isEqualTo(42);
        assertThat(targetState.getMembers()).containsExactlyInAnyOrderElementsOf(MEMBERS);
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);
        assertThat(metrics.numChunksReceived()).isEqualTo(totalChunks);
        assertThat(metrics.numBytesReceived()).isEqualTo(raw.length);
    }

    public void testMetricsGaugesDuringTransfer() throws Exception {
        int testChunkSize = 16;
        int testBatchSize = 16;
        byte[] raw = createPaddedSnapshot(persistentState, 55, testChunkSize * 5);

        Address leader = Util.createRandomAddress("leader");
        TestAsyncSnapshot target = new TestAsyncSnapshot(0);
        PersistentState targetState = new PersistentState();

        AsynchronousSnapshotManager manager = new AsynchronousSnapshotManager(
                createEventLoop(), target, targetState, log, noOpSender(), metrics,
                tempDir, testChunkSize, testBatchSize);

        int totalChunks = (int) Math.ceil((double) raw.length / testChunkSize);

        assertThat(metrics.activeTransferTotalChunks()).isZero();

        long term = 1;
        long lastIndex = 5;
        long lastTerm = 2;

        manager.install(ByteBuffer.allocate(0),
                new SnapshotMetadataRequest(leader, term, lastTerm, lastIndex, raw.length), (idx, t) -> {});

        ByteBuffer firstChunk = ByteBuffer.wrap(raw, 0, Math.min(testChunkSize, raw.length));
        manager.install(firstChunk,
                new SnapshotChunkResponse(term, lastIndex, 0, false), (idx, t) -> {});

        assertThat(metrics.activeTransferTotalChunks()).isEqualTo(totalChunks);
        assertThat(metrics.activeTransferChunksReceived()).isEqualTo(1);
        assertThat(metrics.activeTransferChunksInFlight()).isEqualTo(totalChunks - 1);
        assertThat(metrics.activeTransferHighestRequested()).isEqualTo(totalChunks);

        for (int i = 1; i < totalChunks; i++) {
            int offset = i * testChunkSize;
            int len = Math.min(testChunkSize, raw.length - offset);
            boolean last = (i == totalChunks - 1);

            ByteBuffer chunk = ByteBuffer.wrap(raw, offset, len);
            manager.install(chunk,
                    new SnapshotChunkResponse(term, lastIndex, offset, last), (idx, t) -> {});
        }

        assertThat(metrics.activeTransferTotalChunks()).isZero();
        assertThat(metrics.activeTransferChunksReceived()).isZero();
        assertThat(metrics.activeTransferChunksInFlight()).isZero();
        assertThat(metrics.activeTransferHighestRequested()).isZero();
        assertThat(metrics.activeTransferMissingChunks()).isEmpty();

        assertThat(target.counter).isEqualTo(55);
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);
    }

    private AsynchronousSnapshotManager createManager(TestAsyncSnapshot asyncSnapshot, RaftEventLoop eventLoop) {
        return new AsynchronousSnapshotManager(eventLoop, asyncSnapshot, persistentState, log, noOpSender(), metrics,
                tempDir, 256 * 1024, 16);
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
        return new NoOpSnapshotSender();
    }

    private static class NoOpSnapshotSender implements SnapshotSender {
        @Override
        public void send(Address dest, ByteBuffer snapshot, long lastIndex, long lastTerm) { }

        @Override
        public void sendMetadata(Address dest, long currTerm, long lastIncludedIndex, long lastIncludedTerm, long totalSize) { }

        @Override
        public void sendChunkRequest(Address dest, long currTerm, long lastIncludedIndex, int startChunk, int count) { }

        @Override
        public void sendChunkResponse(Address dest, long currTerm, long lastIncludedIndex, ByteBuffer chunk, long offset, boolean done) { }
    }

    private static class CapturingSnapshotSender extends NoOpSnapshotSender {
        final AtomicInteger chunkRequestCount = new AtomicInteger();

        @Override
        public void sendChunkRequest(Address dest, long currTerm, long lastIncludedIndex, int startChunk, int count) {
            chunkRequestCount.incrementAndGet();
        }
    }

    private static ByteBuffer createSnapshotBuffer(PersistentState ps, int value) throws Exception {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(256, true);
        ps.writeTo(out);
        out.writeInt(value);
        return ByteBuffer.wrap(out.buffer(), 0, out.position());
    }

    private static byte[] createPaddedSnapshot(PersistentState ps, int value, int minSize) throws Exception {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(minSize * 2, true);
        ps.writeTo(out);
        out.writeInt(value);
        while (out.position() < minSize) {
            out.writeInt(out.position());
        }
        byte[] result = new byte[out.position()];
        System.arraycopy(out.buffer(), 0, result, 0, out.position());
        return result;
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
