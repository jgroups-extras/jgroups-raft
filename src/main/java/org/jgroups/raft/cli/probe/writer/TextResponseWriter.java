package org.jgroups.raft.cli.probe.writer;

import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.util.internal.Json;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;

/**
 * A simple writer that outputs responses as plain text key-value pairs.
 *
 * <p>
 * This format flattens the response structure and prints each property on a new line. It is designed for use with standard
 * Unix text processing tools like {@code grep}, {@code awk}, or {@code sed}, where parsing JSON or ASCII tables is cumbersome.
 * </p>
 *
 * <p>
 * Output Format:
 * <pre>
 * --- A (192.168.1.5:7800) ---
 * role=Leader
 * term=5
 * ...
 * </pre>
 * </p>
 *
 * @author José Bolina
 * @since 2.0.0
 */
record TextResponseWriter(PrintWriter out) implements ProbeResponseWriter {
    static final String HEADER_TEMPLATE = "--- %s (%s) ---%n";
    static final String KEY_VALUE_TEMPLATE = "%s=%s%n";

    @Override
    public void accept(Collection<ProbeResponse> responses) {
        for (ProbeResponse res : responses) {
            // Flatten nested structures so every value is addressable by a single key
            Map<String, String> flat = Json.flatten(res.response());
            out.printf(HEADER_TEMPLATE, res.raftId(), res.source());

            flat.forEach((k, v) -> out.printf(KEY_VALUE_TEMPLATE, k, v));
            out.println();
        }
        out.flush();
    }

    @Override
    public ProbeResponseWriterFormat format() {
        return ProbeResponseWriterFormat.TEXT;
    }
}
