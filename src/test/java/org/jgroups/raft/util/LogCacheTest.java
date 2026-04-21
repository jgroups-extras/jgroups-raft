package org.jgroups.raft.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogCacheControl;
import org.jgroups.protocols.raft.LogCapability;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LogCacheTest {

    private static final byte[] DATA = new byte[10];

    private Log underlying;
    private LogCache cache;

    @BeforeMethod
    public void setUp() throws Exception {
        underlying = new InMemoryLog();
        underlying.init("log-cache-test-" + UUID.randomUUID(), null);
        cache = new LogCache(underlying, 64);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (cache != null)
            cache.close();
    }

    public void testFindCapabilityReturnsLogCacheControl() {
        LogCacheControl control = cache.findCapability(LogCacheControl.class);
        assertThat(control).isNotNull();
    }

    public void testFindCapabilityReturnsNullOnPlainLog() {
        LogCacheControl control = underlying.findCapability(LogCacheControl.class);
        assertThat(control).isNull();
    }

    public void testFindCapabilityDelegatesToWrappedLog() {
        UnknownCapability result = cache.findCapability(UnknownCapability.class);
        assertThat(result).isNull();
    }

    public void testGetPopulatesCache() throws Exception {
        appendEntries(1, 1);

        cache.get(1);
        cache.get(1);

        assertThat(cache.numAccesses()).isEqualTo(2);
        assertThat(cache.hitRatio()).isGreaterThan(0);
    }

    public void testPassthroughGetBypassesCache() throws Exception {
        appendEntries(1, 1);

        cache.disable();

        LogEntry entry = cache.get(1);
        assertThat(entry).isNotNull();
        assertThat(cache.numAccesses()).isZero();
    }

    public void testPassthroughAppendBypassesCache() throws Exception {
        cache.disable();

        appendEntries(1, 5);

        assertThat(cache.cacheSize()).isZero();
        assertThat(cache.lastAppended()).isEqualTo(5);
    }

    public void testEnableRestoresCaching() throws Exception {
        appendEntries(1, 3);
        cache.disable();
        cache.enable(64);

        appendEntries(4, 5);
        cache.get(4);
        cache.get(4);

        assertThat(cache.numAccesses()).isEqualTo(2);
        assertThat(cache.hitRatio()).isGreaterThan(0);
    }

    public void testDisableClearsCache() throws Exception {
        appendEntries(1, 5);
        assertThat(cache.cacheSize()).isGreaterThan(0);

        cache.disable();

        assertThat(cache.cacheSize()).isZero();
    }

    public void testPassthroughForEachDelegatesToUnderlying() throws Exception {
        appendEntries(1, 5);
        cache.disable();

        AtomicInteger count = new AtomicInteger();
        cache.forEach((entry, index) -> count.incrementAndGet());

        assertThat(count.get()).isEqualTo(5);
    }

    public void testPassthroughTrimIsNoOp() throws Exception {
        appendEntries(1, 5);
        int trimsBefore = cache.numTrims();

        cache.disable();
        cache.trim();

        assertThat(cache.numTrims()).isEqualTo(trimsBefore);
    }

    public void testDescriptionReflectsPassthroughMode() {
        String enabled = cache.description();
        assertThat(enabled).doesNotContain("passthrough");

        cache.disable();

        String disabled = cache.description();
        assertThat(disabled).contains("passthrough");
    }

    public void testDescriptionReflectsEnabledMode() {
        cache.disable();
        cache.enable(64);

        String description = cache.description();
        assertThat(description).doesNotContain("passthrough");
    }

    public void testResetStatsClearsCounters() throws Exception {
        appendEntries(1, 3);
        cache.get(1);
        cache.get(1);

        cache.resetStats();

        assertThat(cache.numAccesses()).isZero();
        assertThat(cache.numTrims()).isZero();
        assertThat(cache.hitRatio()).isEqualTo(0.0);
    }

    public void testMaxSizeUpdate() {
        assertThat(cache.maxSize()).isEqualTo(64);

        cache.maxSize(128);

        assertThat(cache.maxSize()).isEqualTo(128);
    }

    public void testEnableSetsMaxSize() {
        cache.disable();
        cache.enable(256);

        assertThat(cache.maxSize()).isEqualTo(256);
    }

    public void testClearEvictsWithoutDisabling() throws Exception {
        appendEntries(1, 5);
        assertThat(cache.cacheSize()).isGreaterThan(0);

        cache.clear();

        assertThat(cache.cacheSize()).isZero();

        appendEntries(6, 8);
        cache.get(6);
        cache.get(6);
        assertThat(cache.hitRatio()).isGreaterThan(0);
    }

    public void testPassthroughStillDelegatesMetadata() throws Exception {
        cache.disable();

        cache.currentTerm(5);
        assertThat(cache.currentTerm()).isEqualTo(5);

        cache.votedFor(null);
        assertThat(cache.votedFor()).isNull();
    }

    public void testDataIntegrityThroughPassthroughToggle() throws Exception {
        appendEntries(1, 5);
        cache.commitIndex(3);
        cache.disable();

        assertThat(cache.lastAppended()).isEqualTo(5);
        assertThat(cache.commitIndex()).isEqualTo(3);

        for (int i = 1; i <= 5; i++) {
            assertThat(cache.get(i)).isNotNull();
        }

        cache.enable(64);

        for (int i = 1; i <= 5; i++) {
            assertThat(cache.get(i)).isNotNull();
        }
    }

    private void appendEntries(long startIndex, int count) throws Exception {
        LogEntries entries = new LogEntries();
        for (int i = 0; i < count; i++) {
            entries.add(new LogEntry(1, DATA));
        }
        cache.append(startIndex, entries);
    }

    private interface UnknownCapability extends LogCapability { }
}
