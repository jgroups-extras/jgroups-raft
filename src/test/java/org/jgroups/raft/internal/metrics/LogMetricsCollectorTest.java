package org.jgroups.raft.internal.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.metrics.LogMetrics;

import java.util.UUID;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link LogMetricsCollector}.
 *
 * <p>
 * Exercises the collector against a standalone RAFT protocol with an {@link InMemoryLog},
 * verifying that all metrics correctly delegate to the protocol and log state.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LogMetricsCollectorTest {

    private RAFT raft;
    private Log log;
    private LogMetrics metrics;

    @BeforeMethod
    public void setup() throws Exception {
        raft = new RAFT();
        log = new InMemoryLog();
        log.init("log-metrics-test-" + UUID.randomUUID(), null);
        raft.log(log);
        metrics = new LogMetricsCollector(raft);
    }

    @AfterMethod
    public void teardown() throws Exception {
        if (log != null)
            log.close();
    }

    public void testEmptyLog() {
        assertThat(metrics.getTotalLogEntries()).isZero();
        assertThat(metrics.getCommittedLogEntries()).isZero();
        assertThat(metrics.getUncommittedLogEntries()).isZero();
        assertThat(metrics.getCommitIndex()).isZero();
    }

    public void testAfterAppends() {
        appendEntries(1, 3);

        assertThat(metrics.getTotalLogEntries()).isEqualTo(3);
        assertThat(metrics.getUncommittedLogEntries()).isEqualTo(3);
        assertThat(metrics.getCommittedLogEntries()).isZero();
    }

    public void testAfterCommit() {
        appendEntries(1, 5);
        log.commitIndex(3);

        assertThat(metrics.getTotalLogEntries()).isEqualTo(5);
        assertThat(metrics.getCommittedLogEntries()).isEqualTo(3);
        assertThat(metrics.getUncommittedLogEntries()).isEqualTo(2);
        assertThat(metrics.getCommitIndex()).isEqualTo(3);
    }

    public void testFullyCommitted() {
        appendEntries(1, 5);
        log.commitIndex(5);

        assertThat(metrics.getCommittedLogEntries()).isEqualTo(5);
        assertThat(metrics.getUncommittedLogEntries()).isZero();
    }

    public void testAfterTruncation() {
        appendEntries(1, 10);
        log.commitIndex(10);

        // Truncate entries 1-5, keeping 6-10.
        log.truncate(6);

        assertThat(metrics.getTotalLogEntries()).isEqualTo(5);
        assertThat(metrics.getCommittedLogEntries()).isEqualTo(5);
        assertThat(metrics.getUncommittedLogEntries()).isZero();
    }

    public void testCurrentTerm() {
        log.currentTerm(3);

        assertThat(metrics.getCurrentTerm()).isEqualTo(3);
    }

    public void testLogSizeInBytes() {
        assertThat(metrics.getLogSizeInBytes()).isZero();

        appendEntries(1, 5);

        assertThat(metrics.getLogSizeInBytes()).isGreaterThan(0);
    }

    public void testSnapshotCounts() {
        assertThat(metrics.getSnapshotCount()).isZero();
        assertThat(metrics.getSnapshotsReceived()).isZero();
    }

    public void testMetricsReflectLiveState() {
        // Verify lazy delegation -- metrics update as log state changes.
        assertThat(metrics.getTotalLogEntries()).isZero();

        appendEntries(1, 3);
        assertThat(metrics.getTotalLogEntries()).isEqualTo(3);

        log.commitIndex(2);
        assertThat(metrics.getCommittedLogEntries()).isEqualTo(2);
        assertThat(metrics.getUncommittedLogEntries()).isEqualTo(1);
    }

    private void appendEntries(long startIndex, int count) {
        LogEntries entries = new LogEntries();
        for (int i = 0; i < count; i++) {
            entries.add(new LogEntry(1, new byte[]{(byte) i}));
        }
        log.append(startIndex, entries);
    }
}
