package org.jgroups.raft.internal.probe;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * Tests that the {@link MetricCategory} enum defines the correct filterable metric groups
 * and that the keys match what the server actually returns in the metrics response.
 *
 * @author José Bolina
 * @since 2.0
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class MetricCategoryTest {

    /**
     * Verifies that all six metric category keys are defined and have the expected kebab-case format.
     */
    @Test
    public void testAllCategoriesAreDefined() {
        Set<String> expectedKeys = Set.of(
            "election-metrics",
            "total-latency",
            "processing-latency",
            "election-latency",
            "redirect-latency",
            "log-metrics"
        );

        Set<String> actualKeys = Set.of(
            MetricCategory.ELECTION_METRICS.key(),
            MetricCategory.TOTAL_LATENCY.key(),
            MetricCategory.PROCESSING_LATENCY.key(),
            MetricCategory.ELECTION_LATENCY.key(),
            MetricCategory.REDIRECT_LATENCY.key(),
            MetricCategory.LOG_METRICS.key()
        );

        assertThat(actualKeys)
            .as("All six metric categories should be defined with correct kebab-case keys")
            .isEqualTo(expectedKeys);
    }

    /**
     * Verifies that the enum can be used in a switch statement (all values covered).
     */
    @Test
    public void testEnumValuesAreExhaustive() {
        for (MetricCategory category : MetricCategory.values()) {
            String key = switch (category) {
                case ELECTION_METRICS -> "election-metrics";
                case TOTAL_LATENCY -> "total-latency";
                case PROCESSING_LATENCY -> "processing-latency";
                case ELECTION_LATENCY -> "election-latency";
                case REDIRECT_LATENCY -> "redirect-latency";
                case LOG_METRICS -> "log-metrics";
            };

            assertThat(category.key())
                .as("Each category key should match expected value")
                .isEqualTo(key);
        }
    }
}
