package org.jgroups.raft.metrics;

/**
 * Latency measurements for the main operations in a Raft cluster.
 *
 * <p>
 * Provides visibility into how long different types of operations take, which helps identify bottlenecks and monitor
 * cluster health under load. Four categories of latency are tracked, each returned as a {@link LatencyMetrics} distribution:
 * </p>
 *
 * <ul>
 *     <li><b>Total</b>: end-to-end request latency as experienced by the caller.</li>
 *     <li><b>Processing</b>: consensus latency, isolating the state machine apply phase.</li>
 *     <li><b>Election</b>: time to elect a new leader after a failure.</li>
 *     <li><b>Redirect</b>: round-trip cost when a follower forwards a request to the leader.</li>
 * </ul>
 *
 * <h2>Diagnosing Bottlenecks</h2>
 *
 * <p>
 * Start with {@link #getTotalLatency()} to understand the latency users experience. If total latency is high, compare it
 * against {@link #getProcessingLatency()} to determine whether the delay is in consensus or elsewhere (e.g., queuing before
 * processing). On follower nodes, check {@link #getRedirectLatency()} to measure how much the forwarding hop contributes
 * to overall latency. Use {@link #getLeaderElectionLatency()} to assess how quickly the cluster recovers after a leader failure.
 * </p>
 *
 * <h2>Role-Specific Availability</h2>
 *
 * <p>
 * Not all metrics are available on every node. Total and processing latency are only recorded on the leader, since it is
 * the node that processes requests. Redirect latency is only recorded on followers and learners, since they are the ones
 * forwarding requests. Election latency is recorded on the JGroups coordinator that runs the voting process.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see LatencyMetrics
 */
public interface PerformanceMetrics {

    /**
     * End-to-end latency from request submission to response.
     *
     * <p>
     * Covers the full lifecycle of a request, including any internal queuing, consensus, and state machine handling.
     * This is the latency most relevant to application-level SLAs; if this metric degrades, users are experiencing slower
     * responses. Compare against {@link #getProcessingLatency()} to determine whether the bottleneck is in the consensus
     * phase or outside it.
     * </p>
     *
     * <p>
     * Only recorded on the leader node.
     * </p>
     *
     * @return the total operation latency distribution.
     */
    LatencyMetrics getTotalLatency();

    /**
     * Latency of the consensus process.
     *
     * <p>
     * Measures the time spent achieving majority agreement, from when the request starts being processed until it is committed.
     * When significantly lower than {@link #getTotalLatency()}, the gap reveals time spent in queuing or applying the
     * command to the state machine. When both metrics are close, consensus is the dominant cost and network or cluster
     * size may be the limiting factor.
     * </p>
     *
     * <p>
     * Only recorded on the leader node.
     * </p>
     *
     * @return the consensus processing latency distribution.
     */
    LatencyMetrics getProcessingLatency();

    /**
     * Latency of leader elections.
     *
     * <p>
     * Measures how long it takes to elect a new leader after the previous one becomes unavailable. During this window
     * the cluster cannot accept any operations, so shorter elections mean less downtime. High election latency may indicate
     * network delays between cluster members or contention in the voting process.
     * </p>
     *
     * <p>
     * Only recorded on the JGroups coordinator that runs the voting process.
     * </p>
     *
     * @return the election latency distribution.
     */
    LatencyMetrics getLeaderElectionLatency();

    /**
     * Round-trip latency for requests forwarded from a non-leader node to the leader.
     *
     * <p>
     * When a client submits a request to a follower or learner, the request is transparently forwarded to the leader for
     * processing and the response is relayed back. This metric captures the full round-trip cost of that forwarding.
     * High redirect latency compared to the leader's total latency points to network overhead between the forwarding node
     * and the leader.
     * </p>
     *
     * <p>
     * Only recorded on follower and learner nodes.
     * </p>
     *
     * @return the redirect latency distribution.
     */
    LatencyMetrics getRedirectLatency();
}
