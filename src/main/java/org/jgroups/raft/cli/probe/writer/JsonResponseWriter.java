package org.jgroups.raft.cli.probe.writer;

import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.util.internal.Json;

import java.io.PrintWriter;
import java.util.Collection;

/**
 * A writer that outputs responses as a raw JSON array.
 *
 * <p>
 * This format is ideal for:
 * <ul>
 *   <li>Programmatic parsing (piping to {@code jq} or scripts).</li>
 *   <li>Logging and archival purposes.</li>
 * </ul>
 *
 * It constructs a valid JSON array {@code [ {...}, {...} ]} containing all responses.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
record JsonResponseWriter(PrintWriter out) implements ProbeResponseWriter {

    @Override
    public void accept(Collection<ProbeResponse> responses) {
        // Start Array '['
        out.print(Json.LSB);
        boolean first = true;
        for (ProbeResponse response : responses) {
            // Comma separator ',' between elements.
            if (!first)
                out.print(Json.COMMA);

            first = false;

            // Delegate individual object serialization to the ProbeResponse implementation.
            out.print(response.toJson());
        }

        // End Array ']'
        out.print(Json.RSB);
        out.flush();
    }

    @Override
    public ProbeResponseWriterFormat format() {
        return ProbeResponseWriterFormat.JSON;
    }
}
