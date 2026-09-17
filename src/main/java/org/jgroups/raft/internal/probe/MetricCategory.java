package org.jgroups.raft.internal.probe;

/**
 * Enumeration of filterable metric categories returned by the {@code raft-metrics} probe.
 *
 * @author José Bolina
 * @since 2.0
 */
public enum MetricCategory {
    /**
     * Leader election information: current leader ID, election time, time since last change.
     */
    ELECTION_METRICS("election-metrics"),

    /**
     * End-to-end latency statistics for processing a user request (from receive to commit).
     */
    TOTAL_LATENCY("total-latency"),

    /**
     * Processing latency statistics (local computation only, excludes replication wait).
     */
    PROCESSING_LATENCY("processing-latency"),

    /**
     * Leader election latency statistics (time to complete an election round).
     */
    ELECTION_LATENCY("election-latency"),

    /**
     * Redirect latency statistics (follower → leader forwarding overhead).
     */
    REDIRECT_LATENCY("redirect-latency"),

    /**
     * Log metrics: entry counts, log size, commit index, snapshot statistics.
     */
    LOG_METRICS("log-metrics");

    private final String key;

    MetricCategory(String key) {
        this.key = key;
    }

    /**
     * Returns the kebab-case key used in the metrics JSON response.
     *
     * <p>
     * This key is what users type for the {@code -category} CLI option and what appears s bracketed table headers in
     * the output.
     * </p>
     *
     * @return the category key (e.g., {@code "election-metrics"})
     */
    public String key() {
        return key;
    }
}
