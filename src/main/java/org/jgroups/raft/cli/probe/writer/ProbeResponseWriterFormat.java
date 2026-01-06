package org.jgroups.raft.cli.probe.writer;

import org.jgroups.raft.cli.probe.ProbeResponseWriter;

import java.io.PrintWriter;

/**
 * Enumeration of supported CLI output formats.
 *
 * <p>
 * This enum acts as a <b>Factory</b> for {@link ProbeResponseWriter} instances. It allows commands to easily instantiate
 * the correct writer based on the user's input (e.g., {@code --format=JSON}).
 * </p>
 *
 * @author José Bolina
 * @since 2.0.0
 */
public enum ProbeResponseWriterFormat {

    /**
     * Renders data in a human-readable ASCII table.
     *
     * <p>
     * This is the default format, best suited for interactive terminal usage. It automatically handles column sizing
     * and ANSI coloring.
     * </p>
     */
    TABLE {
        @Override
        public ProbeResponseWriter writer(PrintWriter out) {
            return new TableResponseWriter(out);
        }
    },

    /**
     * Renders data as simple key-value pairs or raw text lines.
     *
     * <p>
     * Best suited for simple piping (`grep`, `awk`) where structured JSON is overkill but visual tables are hard to parse.
     * </p>
     */
    TEXT {
        @Override
        public ProbeResponseWriter writer(PrintWriter out) {
            return new TextResponseWriter(out);
        }
    },

    /**
     * Renders data as a valid JSON array.
     *
     * <p>
     * Best suited for programmatic processing (e.g., piping to `jq` or external monitoring tools).
     * </p>
     */
    JSON {
        @Override
        public ProbeResponseWriter writer(PrintWriter out) {
            return new JsonResponseWriter(out);
        }
    };

    /**
     * Factory method to create a writer instance for this format.
     *
     * @param out The output stream where the writer should print.
     * @return A concrete implementation of {@link ProbeResponseWriter}.
     */
    public abstract ProbeResponseWriter writer(PrintWriter out);
}
