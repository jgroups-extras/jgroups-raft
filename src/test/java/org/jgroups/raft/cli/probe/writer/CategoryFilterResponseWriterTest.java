package org.jgroups.raft.cli.probe.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.internal.probe.MetricCategory.PROCESSING_LATENCY;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.internal.probe.MetricCategory;
import org.jgroups.util.UUID;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

/**
 * Tests for {@link CategoryFilterResponseWriter}, verifying that it correctly filters
 * probe responses to a single metric category while preserving identity fields.
 *
 * @author José Bolina
 * @since 2.0
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CategoryFilterResponseWriterTest {

    /**
     * Verifies that the filter trims the response map to only the requested category key.
     */
    @Test
    public void testFilterTrimsToSingleCategory() {
        // Setup: Create a response with multiple categories
        Map<String, Object> fullMetrics = new HashMap<>();
        fullMetrics.put("total-nodes", 3);
        fullMetrics.put("active-nodes", 3);
        fullMetrics.put("election-metrics", Map.of("leader-raft-id", "A"));
        fullMetrics.put("processing-latency", Map.of("avg-latency", "10ms"));
        fullMetrics.put("log-metrics", Map.of("total-entries", 100));

        Address addr = UUID.randomUUID();
        ProbeResponseWriter.ProbeResponse originalResponse =
                new ProbeResponseWriter.ProbeResponse("node-A", addr, fullMetrics);

        // Create a capturing delegate to verify what gets forwarded
        CapturingWriter delegate = new CapturingWriter();
        CategoryFilterResponseWriter filter = new CategoryFilterResponseWriter(delegate, PROCESSING_LATENCY);

        // Execute
        filter.accept(List.of(originalResponse));

        // Verify: delegate received a response with only the requested category
        assertThat(delegate.capturedResponses)
                .as("Filter should forward exactly one response")
                .hasSize(1);

        ProbeResponseWriter.ProbeResponse filtered = delegate.capturedResponses.get(0);

        assertThat(filtered.raftId())
                .as("Identity field raft-id should be preserved")
                .isEqualTo("node-A");

        assertThat(filtered.source())
                .as("Identity field source should be preserved")
                .isEqualTo(addr);

        assertThat(filtered.response())
                .as("Response map should contain only the requested category")
                .containsOnlyKeys("processing-latency");

        assertThat(filtered.response().get("processing-latency"))
                .as("Category value should be preserved")
                .isEqualTo(Map.of("avg-latency", "10ms"));
    }

    /**
     * Verifies that scalar fields (total-nodes, active-nodes) are dropped when a filter is active.
     */
    @Test
    public void testFilterDropsScalarFields() {
        Map<String, Object> fullMetrics = new HashMap<>();
        fullMetrics.put("total-nodes", 3);
        fullMetrics.put("active-nodes", 3);
        fullMetrics.put("log-metrics", Map.of("total-entries", 100));

        Address addr = UUID.randomUUID();
        ProbeResponseWriter.ProbeResponse originalResponse =
                new ProbeResponseWriter.ProbeResponse("node-B", addr, fullMetrics);

        CapturingWriter delegate = new CapturingWriter();
        CategoryFilterResponseWriter filter = new CategoryFilterResponseWriter(delegate, MetricCategory.LOG_METRICS);

        filter.accept(List.of(originalResponse));

        ProbeResponseWriter.ProbeResponse filtered = delegate.capturedResponses.get(0);

        assertThat(filtered.response())
                .as("Scalar fields should be excluded when filtering")
                .doesNotContainKeys("total-nodes", "active-nodes")
                .containsOnlyKeys("log-metrics");
    }

    /**
     * Verifies that the filter handles multiple responses (multi-node cluster).
     */
    @Test
    public void testFilterHandlesMultipleResponses() {
        Map<String, Object> metricsA = Map.of(
                "election-metrics", Map.of("leader-raft-id", "A"),
                "processing-latency", Map.of("avg-latency", "5ms")
        );
        Map<String, Object> metricsB = Map.of(
                "election-metrics", Map.of("leader-raft-id", "A"),
                "processing-latency", Map.of("avg-latency", "8ms")
        );

        Address addrA = UUID.randomUUID();
        Address addrB = UUID.randomUUID();
        List<ProbeResponseWriter.ProbeResponse> responses = List.of(
                new ProbeResponseWriter.ProbeResponse("node-A", addrA, metricsA),
                new ProbeResponseWriter.ProbeResponse("node-B", addrB, metricsB)
        );

        CapturingWriter delegate = new CapturingWriter();
        CategoryFilterResponseWriter filter = new CategoryFilterResponseWriter(delegate, MetricCategory.ELECTION_METRICS);

        filter.accept(responses);

        assertThat(delegate.capturedResponses)
                .as("All responses should be filtered and forwarded")
                .hasSize(2);

        assertThat(delegate.capturedResponses.get(0).response())
                .containsOnlyKeys("election-metrics");
        assertThat(delegate.capturedResponses.get(1).response())
                .containsOnlyKeys("election-metrics");
    }

    /**
     * Verifies that format() and out() pass through to the delegate.
     */
    @Test
    public void testPassThroughMethods() {
        CapturingWriter delegate = new CapturingWriter();
        CategoryFilterResponseWriter filter = new CategoryFilterResponseWriter(delegate, PROCESSING_LATENCY);

        assertThat(filter.format())
                .as("format() should delegate")
                .isEqualTo(delegate.format());

        assertThat(filter.out())
                .as("out() should delegate")
                .isSameAs(delegate.out());
    }

    /**
     * Simple test writer that captures accepted responses for verification.
     */
    private static class CapturingWriter implements ProbeResponseWriter {
        final List<ProbeResponseWriter.ProbeResponse> capturedResponses = new java.util.ArrayList<>();
        final StringWriter output = new StringWriter();
        final PrintWriter writer = new PrintWriter(output);

        @Override
        public void accept(Collection<ProbeResponse> responses) {
            capturedResponses.addAll(responses);
        }

        @Override
        public ProbeResponseWriterFormat format() {
            return ProbeResponseWriterFormat.TEXT;
        }

        @Override
        public PrintWriter out() {
            return writer;
        }
    }
}
