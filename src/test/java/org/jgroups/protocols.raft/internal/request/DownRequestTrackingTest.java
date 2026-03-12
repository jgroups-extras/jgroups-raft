package org.jgroups.protocols.raft.internal.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import org.jgroups.Global;
import org.jgroups.raft.internal.metrics.RaftProtocolMetrics;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.tests.harness.ControlledTimeService;

import java.util.concurrent.CompletableFuture;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link DownRequest} latency tracking with controlled time.
 *
 * <p>
 * Verifies that:
 * <ul>
 *   <li>Total latency records the interval from {@code startTotal()} to {@code complete()}.</li>
 *   <li>Processing latency records the interval from {@code startProcessing()} to {@code completeProcessing()}.</li>
 *   <li>The two metrics are independent and recorded at different completion points.</li>
 *   <li>Untracked requests (metrics disabled) produce no recordings.</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class DownRequestTrackingTest {

    private ControlledTimeService timeService;
    private RaftProtocolMetrics metrics;

    @BeforeMethod
    void setUp() {
        timeService = new ControlledTimeService();
        metrics = new RaftProtocolMetrics();
    }

    private DownRequest createTracked() {
        return DownRequest.create(
                new CompletableFuture<>(), new byte[0], 0, 0,
                false, null, false, metrics, timeService
        );
    }

    private DownRequest createUntracked() {
        return DownRequest.create(
                new CompletableFuture<>(), new byte[0], 0, 0,
                false, null, false, null, null
        );
    }

    /**
     * Total latency: startTotal → complete.
     * Processing latency: startProcessing → completeProcessing.
     *
     * <p>
     * Simulates the real flow:
     * <ol>
     *   <li>offer() calls startTotal (t=0)</li>
     *   <li>Time advances 10ms (queue wait)</li>
     *   <li>process() calls startProcessing (t=10ms)</li>
     *   <li>Time advances 5ms (log append + replication)</li>
     *   <li>RequestTable.Entry.add() reaches majority → completeProcessing (t=15ms)</li>
     *   <li>Time advances 2ms (state machine apply)</li>
     *   <li>complete() → completeTotal (t=17ms)</li>
     * </ol>
     * Expected: total = 17ms, processing = 5ms.
     * </p>
     */
    public void testFullLifecycle() {
        DownRequest dr = createTracked();

        // offer() → startTotal at t=0
        dr.startTotal();

        // Queue wait: 10ms
        timeService.advance(10_000_000);

        // process() → startProcessing at t=10ms
        dr.startProcessing();

        // Log append + replication: 5ms
        timeService.advance(5_000_000);

        // Majority reached → completeProcessing at t=15ms
        dr.completeProcessing();

        // State machine apply: 2ms
        timeService.advance(2_000_000);

        // complete() → completeTotal at t=17ms
        dr.complete(new byte[0]);

        LatencyMetrics total = metrics.total();
        LatencyMetrics processing = metrics.processing();

        assertThat(total.getTotalMeasurements())
                .as("Total latency should have 1 recording")
                .isEqualTo(1);
        assertThat(total.getMaxLatency())
                .as("Total latency should be 17ms (full end-to-end)")
                .isCloseTo(17_000_000.0, withinPercentage(0.1));

        assertThat(processing.getTotalMeasurements())
                .as("Processing latency should have 1 recording")
                .isEqualTo(1);
        assertThat(processing.getMaxLatency())
                .as("Processing latency should be 5ms (event loop to commit)")
                .isCloseTo(5_000_000.0, withinPercentage(0.1));
    }

    /**
     * Processing latency ends before total latency.
     * completeProcessing() is called independently from complete().
     */
    public void testProcessingCompletesBeforeTotal() {
        DownRequest dr = createTracked();

        dr.startTotal();
        timeService.advance(1_000_000);

        dr.startProcessing();
        timeService.advance(3_000_000);

        // Processing completes at t=4ms.
        dr.completeProcessing();

        // Verify processing is recorded immediately.
        assertThat(metrics.processing().getTotalMeasurements()).isEqualTo(1);
        assertThat(metrics.processing().getMaxLatency()).isCloseTo(3_000_000.0, withinPercentage(0.1));

        // Total not yet recorded.
        assertThat(metrics.total().getTotalMeasurements()).isZero();

        // State machine apply takes 7ms.
        timeService.advance(7_000_000);
        dr.complete(new byte[0]);

        // Now total is recorded: 1 + 3 + 7 = 11ms.
        assertThat(metrics.total().getTotalMeasurements()).isEqualTo(1);
        assertThat(metrics.total().getMaxLatency()).isCloseTo(11_000_000.0, withinPercentage(0.1));
    }

    /**
     * Failure also records total latency but processing may or may not have been recorded.
     */
    public void testFailedRequestRecordsTotalLatency() {
        DownRequest dr = createTracked();

        dr.startTotal();
        timeService.advance(5_000_000);

        dr.startProcessing();
        timeService.advance(2_000_000);

        // Request fails before reaching majority (no completeProcessing call).
        dr.failed(new RuntimeException("not leader"));

        // Total should be recorded: 5 + 2 = 7ms.
        assertThat(metrics.total().getTotalMeasurements()).isEqualTo(1);
        assertThat(metrics.total().getMaxLatency()).isCloseTo(7_000_000.0, withinPercentage(0.1));

        // Processing was NOT completed (no majority reached).
        assertThat(metrics.processing().getTotalMeasurements()).isZero();
    }

    /**
     * Failure before processing starts (e.g., queue full in offer()).
     */
    public void testFailedBeforeProcessing() {
        DownRequest dr = createTracked();

        dr.startTotal();
        timeService.advance(1_000_000);

        // Queue full — failed() called directly from offer().
        dr.failed(new IllegalStateException("processing queue is full"));

        assertThat(metrics.total().getTotalMeasurements()).isEqualTo(1);
        assertThat(metrics.total().getMaxLatency()).isCloseTo(1_000_000.0, withinPercentage(0.1));

        assertThat(metrics.processing().getTotalMeasurements()).isZero();
    }

    /**
     * Multiple requests accumulate in the same metrics instance.
     */
    public void testMultipleRequests() {
        for (int i = 1; i <= 3; i++) {
            DownRequest dr = createTracked();

            dr.startTotal();
            timeService.advance(i * 1_000_000L);

            dr.startProcessing();
            timeService.advance(i * 500_000L);

            dr.completeProcessing();

            timeService.advance(100_000);
            dr.complete(new byte[0]);
        }

        assertThat(metrics.total().getTotalMeasurements()).isEqualTo(3);
        assertThat(metrics.processing().getTotalMeasurements()).isEqualTo(3);
    }

    /**
     * Untracked requests produce no recordings.
     */
    public void testUntrackedRequest() {
        DownRequest dr = createUntracked();

        dr.startTotal();
        dr.startProcessing();
        dr.completeProcessing();
        dr.complete(new byte[0]);

        // No metrics instance — nothing to check, just verifying no NPE or side effects.
        assertThat(metrics.total().getTotalMeasurements()).isZero();
        assertThat(metrics.processing().getTotalMeasurements()).isZero();
    }

    /**
     * Read-only requests also track both metrics.
     */
    public void testReadOnlyRequest() {
        DownRequest dr = DownRequest.create(
                new CompletableFuture<>(), new byte[0], 0, 0,
                false, null, true, metrics, timeService
        );

        dr.startTotal();
        timeService.advance(2_000_000);

        dr.startProcessing();
        timeService.advance(1_000_000);

        dr.completeProcessing();

        timeService.advance(500_000);
        dr.complete(new byte[0]);

        assertThat(metrics.total().getTotalMeasurements()).isEqualTo(1);
        assertThat(metrics.total().getMaxLatency()).isCloseTo(3_500_000.0, withinPercentage(0.1));

        assertThat(metrics.processing().getTotalMeasurements()).isEqualTo(1);
        assertThat(metrics.processing().getMaxLatency()).isCloseTo(1_000_000.0, withinPercentage(0.1));
    }
}
