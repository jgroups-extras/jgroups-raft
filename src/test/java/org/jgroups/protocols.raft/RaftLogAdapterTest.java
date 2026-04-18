package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.util.LogCache;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RaftLogAdapterTest {

    private static final byte[] DATA = new byte[10];

    private InMemoryLog underlying;
    private AtomicReference<Throwable> listenerCause;
    private AtomicInteger listenerCount;
    private RaftLogAdapter adapter;

    @BeforeMethod
    public void setUp() throws Exception {
        underlying = new InMemoryLog();
        underlying.init("adapter-test-" + UUID.randomUUID(), null);
        listenerCause = new AtomicReference<>();
        listenerCount = new AtomicInteger();
        adapter = new RaftLogAdapter(underlying, cause -> {
            listenerCause.set(cause);
            listenerCount.incrementAndGet();
        });
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (adapter != null)
            adapter.close();
    }

    public void testDelegatesAppend() {
        appendEntries(1, 3);

        assertThat(adapter.lastAppended()).isEqualTo(3);
        assertThat(adapter.get(1)).isNotNull();
        assertThat(adapter.get(2)).isNotNull();
        assertThat(adapter.get(3)).isNotNull();
    }

    public void testDelegatesCurrentTerm() {
        adapter.currentTerm(5);

        assertThat(adapter.currentTerm()).isEqualTo(5);
        assertThat(underlying.currentTerm()).isEqualTo(5);
    }

    public void testDelegatesVotedFor() {
        adapter.votedFor(null);

        assertThat(adapter.votedFor()).isNull();
    }

    public void testDelegatesCommitIndex() {
        appendEntries(1, 5);
        adapter.commitIndex(3);

        assertThat(adapter.commitIndex()).isEqualTo(3);
        assertThat(underlying.commitIndex()).isEqualTo(3);
    }

    public void testDelegatesForEach() {
        appendEntries(1, 5);

        AtomicInteger count = new AtomicInteger();
        adapter.forEach((entry, index) -> count.incrementAndGet());

        assertThat(count.get()).isEqualTo(5);
    }

    public void testDelegatesTruncate() {
        appendEntries(1, 5);
        adapter.truncate(3);

        assertThat(adapter.firstAppended()).isEqualTo(underlying.firstAppended());
    }

    public void testDelegatesDeleteAllEntriesStartingFrom() {
        appendEntries(1, 5);
        adapter.deleteAllEntriesStartingFrom(3);

        assertThat(adapter.lastAppended()).isEqualTo(2);
    }

    public void testDelegatesSnapshot() {
        ByteBuffer snapshot = ByteBuffer.wrap(new byte[]{1, 2, 3});
        adapter.setSnapshot(snapshot);

        assertThat(adapter.getSnapshot()).isNotNull();
    }

    public void testDelegatesUseFsync() {
        adapter.useFsync(true);

        assertThat(adapter.useFsync()).isTrue();
        assertThat(underlying.useFsync()).isTrue();
    }

    public void testDelegatesSizeInBytes() {
        appendEntries(1, 3);

        assertThat(adapter.sizeInBytes()).isEqualTo(underlying.sizeInBytes());
    }

    public void testDelegatesFindCapability() throws Exception {
        LogCache cache = new LogCache(underlying, 64);
        RaftLogAdapter adapterWithCache = new RaftLogAdapter(cache, cause -> {});

        LogCacheControl control = adapterWithCache.findCapability(LogCacheControl.class);

        assertThat(control).isNotNull();
        adapterWithCache.close();
    }

    public void testFindCapabilityReturnsNullWhenAbsent() {
        LogCacheControl control = adapter.findCapability(LogCacheControl.class);

        assertThat(control).isNull();
    }

    public void testNotPoisonedInitially() {
        assertThat(adapter.isPoisoned()).isFalse();
    }

    public void testPoisonSetsState() {
        adapter.poison(new RuntimeException("disk full"));

        assertThat(adapter.isPoisoned()).isTrue();
    }

    public void testPoisonNotifiesListenerOnce() {
        RuntimeException first = new RuntimeException("disk full");
        RuntimeException second = new RuntimeException("another error");

        adapter.poison(first);
        adapter.poison(second);

        assertThat(listenerCount.get()).isEqualTo(1);
        assertThat(listenerCause.get()).isSameAs(first);
    }

    public void testPoisonedAppendThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> appendEntries(1, 1))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedGetThrows() {
        appendEntries(1, 3);
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.get(1))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedCurrentTermSetterThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.currentTerm(5))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedVotedForSetterThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.votedFor(null))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedCommitIndexSetterThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.commitIndex(1))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedTruncateThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.truncate(1))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedDeleteAllThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.deleteAllEntriesStartingFrom(1))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedForEachThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.forEach((e, i) -> {}))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedForEachWithRangeThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.forEach((e, i) -> {}, 1, 5))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedSetSnapshotThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.setSnapshot(ByteBuffer.wrap(new byte[]{1})))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedGetSnapshotThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.getSnapshot())
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedReinitializeToThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.reinitializeTo(1, new LogEntry(1, DATA)))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedInitThrows() {
        adapter.poison(new RuntimeException("disk full"));

        assertThatThrownBy(() -> adapter.init("test", null))
                .isInstanceOf(RaftLogException.class);
    }

    public void testPoisonedStatusGettersStillDelegate() {
        appendEntries(1, 5);
        adapter.commitIndex(3);
        adapter.currentTerm(7);

        adapter.poison(new RuntimeException("disk full"));

        assertThat(adapter.currentTerm()).isEqualTo(7);
        assertThat(adapter.commitIndex()).isEqualTo(3);
        assertThat(adapter.lastAppended()).isEqualTo(5);
        assertThat(adapter.firstAppended()).isEqualTo(underlying.firstAppended());
        assertThat(adapter.votedFor()).isNull();
        assertThat(adapter.sizeInBytes()).isEqualTo(underlying.sizeInBytes());
    }

    public void testPoisonedUseFsyncStillDelegates() {
        adapter.poison(new RuntimeException("disk full"));

        adapter.useFsync(true);
        assertThat(adapter.useFsync()).isTrue();
    }

    public void testPoisonedCloseStillDelegates() throws Exception {
        adapter.poison(new RuntimeException("disk full"));

        adapter.close();
    }

    public void testPoisonedFindCapabilityStillDelegates() throws Exception {
        LogCache cache = new LogCache(underlying, 64);
        RaftLogAdapter adapterWithCache = new RaftLogAdapter(cache, cause -> {});
        adapterWithCache.poison(new RuntimeException("disk full"));

        LogCacheControl control = adapterWithCache.findCapability(LogCacheControl.class);
        assertThat(control).isNotNull();

        adapterWithCache.close();
    }

    public void testPoisonedThrowsSameInstance() {
        adapter.poison(new RuntimeException("disk full"));

        RaftLogException first = null;
        RaftLogException second = null;
        try {
            adapter.get(1);
        } catch (RaftLogException e) {
            first = e;
        }
        try {
            adapter.currentTerm(1);
        } catch (RaftLogException e) {
            second = e;
        }

        assertThat(first).isNotNull();
        assertThat(first).isSameAs(second);
    }

    public void testToStringHealthy() {
        assertThat(adapter.toString()).contains("healthy");
    }

    public void testToStringPoisoned() {
        adapter.poison(new RuntimeException("disk full"));

        assertThat(adapter.toString()).contains("POISONED");
    }

    private void appendEntries(long startIndex, int count) {
        LogEntries entries = new LogEntries();
        for (int i = 0; i < count; i++) {
            entries.add(new LogEntry(1, DATA));
        }
        adapter.append(startIndex, entries);
    }
}
