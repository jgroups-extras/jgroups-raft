package org.jgroups.raft.cli.probe;

import org.jgroups.Address;
import org.jgroups.raft.cli.probe.writer.ProbeResponseWriterFormat;
import org.jgroups.raft.util.internal.Json;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines the contract for processing and displaying results from a JGroups Probe request.
 *
 * <p>
 * Implementations of this interface are responsible for taking the raw data received from cluster nodes and formatting
 * it for the user (e.g., rendering a table, printing raw JSON, or simple text lines).
 * </p>
 *
 * <p>
 * This follows the <b>Strategy Pattern</b>, allowing the CLI to switch output modes dynamically based on the user's
 * {@code --format} selection.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see ProbeResponseWriterFormat
 */
public interface ProbeResponseWriter {

    // Keys utilized by the ProbeResponse to serialize into JSON.
    // Should NOT be changed.
    String RAFT_ID_KEY = "raft-id";
    String ADDRESS_KEY = "address";
    String RESPONSE_KEY = "response";

    /**
     * Consumes the collection of responses received from the cluster and writes them to the output.
     *
     * @param responses A collection of normalized {@link ProbeResponse} objects containing data from each node.
     */
    void accept(Collection<ProbeResponse> responses);

    /**
     * Returns the format type associated with this writer.
     *
     * @return The {@link ProbeResponseWriterFormat} enum constant
     */
    ProbeResponseWriterFormat format();

    /**
     * Returns the underlying writer used for output.
     *
     * @return The {@link PrintWriter} wrapping standard out.
     */
    PrintWriter out();

    /**
     * A record representing a successful probe response from a single node.
     *
     * @param raftId   The unique Raft ID of the node (e.g., "A", "B").
     * @param source   The physical network address of the node (e.g., "192.168.1.5:7800").
     * @param response The structured data payload returned by the node.
     */
    record ProbeResponse(String raftId, Address source, Map<String, Object> response) {

        @Override
        public Map<String, Object> response() {
            return Collections.unmodifiableMap(response);
        }

        /**
         * Helper to serialize this single response entry back to JSON.
         *
         * @return A JSON string representation of this response.
         */
        public String toJson() {
            Map<String, Object> m = new HashMap<>();
            m.put(RAFT_ID_KEY, raftId);
            m.put(ADDRESS_KEY, source != null ? String.valueOf(source) : null);
            m.put(RESPONSE_KEY, response);
            return Json.toJson(m);
        }
    }
}
