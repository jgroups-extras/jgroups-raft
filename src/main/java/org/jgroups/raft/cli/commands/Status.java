package org.jgroups.raft.cli.commands;

import org.jgroups.raft.internal.probe.RaftProtocolProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Queries the operational status of all Raft nodes in the cluster.
 *
 * <p>
 * This command performs a comprehensive health check by retrieving the internal state of the {@link org.jgroups.protocols.raft.RAFT}
 * protocol from each member. It is the primary tool for monitoring cluster health and diagnosing leadership issues.
 *
 * <p>
 * <b>Output Columns:</b>
 * <ul>
 *   <li><b>raft-id:</b> The unique identifier of the node.</li>
 *   <li><b>role:</b> The current Raft role ({@link org.jgroups.protocols.raft.Leader}, {@link org.jgroups.protocols.raft.Follower},
 *                    or {@link org.jgroups.protocols.raft.Learner}).</li>
 *   <li><b>leader:</b> The Raft ID of the current leader (as seen by each node).</li>
 *   <li><b>term:</b> The current election term (higher is newer).</li>
 *   <li><b>commit-index:</b> The index of the last log entry known to be committed.</li>
 *   <li><b>last-applied:</b> The index of the last log entry applied to the state machine.</li>
 *   <li><b>members:</b> The list of all voting members in the current view. This list does not include learners.</li>
 * </ul>
 * </p>
 *
 * This command supports <b>watch mode</b> ({@code -w}), allowing real-time monitoring of leadership changes and log progression.
 * See {@link WatchableProbeCommand} for more details.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
@Command(name = "status", description = "Comprehensive health check with diagnostic details")
final class Status extends WatchableProbeCommand {

    /**
     * Optional filter to query only a specific node.
     *
     * <p>
     * While the probe request is broadcast to all nodes, providing this ID allows the CLI to filter the display output
     * to focus on a single member's perspective.
     * </p>
     */
    // TODO: implement CLI filtering.
    @Option(names = "-raft-id", description = "Retrieve the status of a specific node identified by the Raft ID", defaultValue = Option.NULL_VALUE)
    private String raftId;

    @Override
    protected String probeRequest() {
        return RaftProtocolProbe.PROBE_RAFT_STATUS;
    }
}
