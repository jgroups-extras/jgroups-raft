package org.jgroups.protocols.raft.internal.snapshot;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.jgroups.Global;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ChunkTrackerTest {

    public void testTotalChunksExactMultiple() {
        ChunkTracker transfer = new ChunkTracker(1024, 256, 16);
        assertThat(transfer.totalChunks()).isEqualTo(4);
    }

    public void testTotalChunksNonExactMultiple() {
        ChunkTracker transfer = new ChunkTracker(1000, 256, 16);
        assertThat(transfer.totalChunks()).isEqualTo(4);
    }

    public void testTotalChunksSingleByte() {
        ChunkTracker transfer = new ChunkTracker(1, 256, 16);
        assertThat(transfer.totalChunks()).isEqualTo(1);
    }

    public void testInitialState() {
        ChunkTracker transfer = new ChunkTracker(1024, 256, 16);
        assertThat(transfer.received()).isZero();
        assertThat(transfer.inFlight()).isZero();
        assertThat(transfer.highestRequested()).isZero();
        assertThat(transfer.bytesReceived()).isZero();
        assertThat(transfer.isComplete()).isFalse();
    }

    public void testMissingChunksEmptyWhenNothingRequested() {
        ChunkTracker transfer = new ChunkTracker(1024, 256, 16);
        assertThat(transfer.missingChunks()).isEmpty();
    }

    public void testMarkReceivedSequential() {
        ChunkTracker tracker = new ChunkTracker(64, 16, 16);
        tracker.markRequested(4);

        tracker.markReceived(0, 16);
        assertThat(tracker.received()).isEqualTo(1);
        assertThat(tracker.bytesReceived()).isEqualTo(16);

        tracker.markReceived(16, 16);
        assertThat(tracker.received()).isEqualTo(2);
        assertThat(tracker.bytesReceived()).isEqualTo(32);
    }

    public void testMarkReceivedOutOfOrder() {
        ChunkTracker tracker = new ChunkTracker(64, 16, 16);
        tracker.markRequested(4);

        tracker.markReceived(48, 16);
        tracker.markReceived(0, 16);
        tracker.markReceived(32, 16);
        tracker.markReceived(16, 16);

        assertThat(tracker.received()).isEqualTo(4);
        assertThat(tracker.isComplete()).isTrue();
    }

    public void testMarkReceivedOutOfBoundsIgnored() {
        ChunkTracker tracker = new ChunkTracker(32, 16, 16);
        tracker.markRequested(2);

        tracker.markReceived(64, 16);

        assertThat(tracker.received()).isZero();
        assertThat(tracker.bytesReceived()).isEqualTo(16);
    }

    public void testDuplicateMarkReceivedIsIdempotent() {
        ChunkTracker tracker = new ChunkTracker(32, 16, 16);
        tracker.markRequested(2);

        tracker.markReceived(0, 16);
        tracker.markReceived(0, 16);

        assertThat(tracker.received()).isEqualTo(1);
        assertThat(tracker.bytesReceived()).isEqualTo(32);
    }

    public void testCompletionRequiresAllChunks() {
        ChunkTracker tracker = new ChunkTracker(48, 16, 16);
        tracker.markRequested(3);

        tracker.markReceived(0, 16);
        assertThat(tracker.isComplete()).isFalse();

        tracker.markReceived(16, 16);
        assertThat(tracker.isComplete()).isFalse();

        tracker.markReceived(32, 16);
        assertThat(tracker.isComplete()).isTrue();
    }

    public void testMarkRequestedCapsAtTotalChunks() {
        ChunkTracker tracker = new ChunkTracker(32, 16, 16);

        tracker.markRequested(16);
        assertThat(tracker.highestRequested()).isEqualTo(2);
        assertThat(tracker.inFlight()).isEqualTo(2);
    }

    public void testShouldRefillAtLowWaterMark() {
        ChunkTracker tracker = new ChunkTracker(320, 16, 8);
        tracker.markRequested(8);

        assertThat(tracker.shouldRefill()).isFalse();

        for (int i = 0; i < 6; i++) tracker.markReceived(i * 16L, 16);
        assertThat(tracker.inFlight()).isEqualTo(2);
        assertThat(tracker.shouldRefill()).isTrue();
    }

    public void testShouldNotRefillWhenAllRequested() {
        ChunkTracker tracker = new ChunkTracker(64, 16, 16);
        tracker.markRequested(16);

        for (int i = 0; i < 3; i++) tracker.markReceived(i * 16L, 16);
        assertThat(tracker.shouldRefill()).isFalse();
    }

    public void testRefillCount() {
        ChunkTracker tracker = new ChunkTracker(320, 16, 8);
        tracker.markRequested(8);

        for (int i = 0; i < 6; i++) tracker.markReceived(i * 16L, 16);

        assertThat(tracker.refillCount()).isEqualTo(6);
    }

    public void testRefillCountCappedByRemainingChunks() {
        ChunkTracker tracker = new ChunkTracker(80, 16, 16);
        tracker.markRequested(16);

        for (int i = 0; i < 4; i++) tracker.markReceived(i * 16L, 16);

        assertThat(tracker.shouldRefill()).isFalse();
        assertThat(tracker.refillCount()).isZero();
    }

    public void testNextRequestStart() {
        ChunkTracker tracker = new ChunkTracker(320, 16, 8);
        assertThat(tracker.nextRequestStart()).isZero();

        tracker.markRequested(8);
        assertThat(tracker.nextRequestStart()).isEqualTo(8);

        tracker.markRequested(4);
        assertThat(tracker.nextRequestStart()).isEqualTo(12);
    }

    public void testMissingChunksWithGaps() {
        ChunkTracker tracker = new ChunkTracker(80, 16, 16);
        tracker.markRequested(5);

        tracker.markReceived(0, 16);
        tracker.markReceived(32, 16);
        tracker.markReceived(64, 16);

        assertThat(tracker.missingChunks()).containsExactly(1, 3);
    }

    public void testMissingChunksOnlyBelowFrontier() {
        ChunkTracker tracker = new ChunkTracker(160, 16, 4);
        tracker.markRequested(4);

        tracker.markReceived(0, 16);
        tracker.markReceived(32, 16);

        assertThat(tracker.missingChunks()).containsExactly(1, 3);
    }

    public void testMissingChunksEmptyWhenAllReceived() {
        ChunkTracker tracker = new ChunkTracker(48, 16, 16);
        tracker.markRequested(3);

        for (int i = 0; i < 3; i++) tracker.markReceived(i * 16L, 16);

        assertThat(tracker.missingChunks()).isEmpty();
    }

    @DataProvider
    public Object[][] slidingWindowConfigurations() {
        return new Object[][] {
                // totalSize, chunkSize, batchSize
                { 160L,  16, 4 },
                { 320L,  16, 8 },
                { 1L,    16, 16 },
                { 1000L, 37, 5 },
                { 77L,   10, 7 },
                { 255L,  17, 6 },
                { 100L,  13, 3 },
                { 999L,  100, 11 },
                { 997L,  31, 13 },
        };
    }

    @Test(dataProvider = "slidingWindowConfigurations")
    public void testSlidingWindowFullLifecycle(long totalSize, int chunkSize, int batchSize) {
        ChunkTracker tracker = new ChunkTracker(totalSize, chunkSize, batchSize);
        int totalChunks = tracker.totalChunks();
        int lowWaterMark = batchSize >> 2;

        int initialBatch = Math.min(batchSize, totalChunks);
        tracker.markRequested(initialBatch);
        assertThat(tracker.highestRequested()).isEqualTo(initialBatch);
        assertThat(tracker.inFlight()).isEqualTo(initialBatch);

        for (int i = 0; i < totalChunks; i++) {
            long offset = (long) i * chunkSize;
            int bytes = (int) Math.min(chunkSize, totalSize - offset);
            tracker.markReceived(offset, bytes);

            if (tracker.isComplete())
                break;

            if (tracker.shouldRefill()) {
                assertThat(tracker.inFlight()).isLessThanOrEqualTo(lowWaterMark);
                int count = tracker.refillCount();
                assertThat(count).isGreaterThan(0);
                tracker.markRequested(count);
                assertThat(tracker.inFlight()).isLessThanOrEqualTo(batchSize);
            }
        }

        assertThat(tracker.isComplete()).isTrue();
        assertThat(tracker.received()).isEqualTo(totalChunks);
        assertThat(tracker.inFlight()).isZero();
        assertThat(tracker.shouldRefill()).isFalse();
        assertThat(tracker.missingChunks()).isEmpty();
        assertThat(tracker.bytesReceived()).isEqualTo(totalSize);
    }
}
