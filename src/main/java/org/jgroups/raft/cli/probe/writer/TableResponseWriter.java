package org.jgroups.raft.cli.probe.writer;

import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.util.internal.Json;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import picocli.CommandLine;

/**
 * A response writer that renders probe data into ASCII tables.
 *
 * <p>
 * This is the default output format for the CLI. It is designed to handle complex, nested JSON responses by dynamically
 * splitting them into multiple tables (e.g., separating "General" stats from "Processing Metrics").
 * </p>
 *
 * <p>
 * <b>Key Features:</b>
 * <ul>
 *   <li><b>Dynamic Column Sizing:</b> Automatically adjusts column widths based on the longest value.</li>
 *   <li><b>Nested Data Handling:</b> Flattens nested maps into separate tables to avoid very-lage cells.</li>
 *   <li><b>ANSI Color Support:</b> Uses Picocli's ANSI helper to render borders and headers correctly on color-enabled terminals.</li>
 *   <li><b>Unicode Box Drawing:</b> Uses high-quality box-drawing characters (┌, ─, ┐) to draw the tables.</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
record TableResponseWriter(PrintWriter out) implements ProbeResponseWriter {

    private static final CommandLine.Help.Ansi ANSI = CommandLine.Help.Ansi.AUTO;

    // Package-private for testing.
    static final String DEFAULT_ROOT_TABLE = "General";

    /**
     * Renders the collection of responses into one or more ASCII tables.
     *
     * <p>
     * The logic follows these steps:
     * <ol>
     * <li>Iterate through all responses to build a schema of columns.</li>
     * <li>Identify nested maps (like "metrics") and extract them into separate named tables.</li>
     * <li>Compute the maximum width required for each column.</li>
     * <li>Draw the tables with headers, borders, and padded cell data.</li>
     * </ol>
     * </p>
     *
     * @param responses The data received from the cluster nodes.
     */
    @Override
    public void accept(Collection<ProbeResponse> responses) {
        if (responses.isEmpty()) {
            out.println(ANSI.string("@|yellow No responses received.|@"));
            return;
        }

        // Store data separated by "category" (e.g., 'General', 'replication-metrics', etc.)
        Map<String, List<Map<String, String>>> tables = new LinkedHashMap<>();
        tables.put(DEFAULT_ROOT_TABLE, new ArrayList<>());
        boolean hasMultipleTables = false;
        for (ProbeResponse response : responses) {
            String raftId = response.raftId();
            // Format address with color for better visibility
            String address = response.source() != null ? String.format("@|yellow %s|@", response.source()) : "-";

            Map<String, String> root = new HashMap<>();
            root.put(RAFT_ID_KEY, raftId);
            root.put(ADDRESS_KEY, address);

            for (Map.Entry<String, Object> entry : response.response().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Map<?, ?> m) {
                    tables.putIfAbsent(key, new ArrayList<>());

                    @SuppressWarnings("unchecked")
                    Map<String, String> nested = Json.flatten((Map<String, ?>) m);
                    // Inherit identity columns so we know who this metric belongs to
                    nested.put(RAFT_ID_KEY, response.raftId());
                    nested.put(ADDRESS_KEY, address);

                    tables.get(key).add(nested);
                    hasMultipleTables = true;
                } else {
                    String v = value == null ? "-" : String.valueOf(value);
                    root.put(key, v);
                }
            }

            // Only add to 'General' table if there are actual properties besides ID/Address
            if (root.size() > 2) {
                tables.get(DEFAULT_ROOT_TABLE).add(root);
            }
        }

        // Render each category as a separate table
        // We render a "header" with the table category and then the actual values.
        // There is also a few lines between each table.
        for (Map.Entry<String, List<Map<String, String>>> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, String>> rows = entry.getValue();

            if (rows.isEmpty()) continue;

            // Print table title if we are showing more than just the default view
            if (hasMultipleTables)
                out.println(ANSI.string(String.format("@|bold,cyan [%s]|@", tableName)));

            // Collect all unique keys to form the header row
            // Always insert the raft-id and addresses first to keep the insertion order.
            // This ensures we see these columns first in the table.
            Set<String> headers = new LinkedHashSet<>();
            headers.add(RAFT_ID_KEY);
            headers.add(ADDRESS_KEY);
            for (Map<String, String> row : rows) {
                headers.addAll(row.keySet());
            }

            TableWriter writer = new TableWriter(ANSI);
            headers.forEach(writer::addHeader);

            for (Map<String, String> row : rows) {
                writer.addRow(row);
            }

            writer.print(out);
        }
    }

    @Override
    public ProbeResponseWriterFormat format() {
        return ProbeResponseWriterFormat.TABLE;
    }

    /**
     * Internal helper class responsible for the actual string manipulation and drawing the ASCII table.
     */
    static final class TableWriter {
        // Package-private for testing.
        // Box Drawing Characters (Unicode)
        static final String TL = "┌"; // Top Left
        static final String TR = "┐"; // Top Right
        static final String BL = "└"; // Bottom Left
        static final String BR = "┘"; // Bottom Right
        static final String H = "─"; // Horizontal
        static final String V = "│"; // Vertical
        static final String T = "┬"; // Top Join
        static final String B = "┴"; // Bottom Join
        static final String C = "┼"; // Cross
        static final String L = "├"; // Left Join
        static final String R = "┤"; // Right Join
        static final String S = " ";

        private final List<String> headers = new ArrayList<>();
        private final List<List<String>> rows = new ArrayList<>();
        private final CommandLine.Help.Ansi ansi;

        public TableWriter(CommandLine.Help.Ansi ansi) {
            this.ansi = ansi;
        }

        public void addHeader(String header) {
            headers.add(header);
        }

        public void addRow(Map<String, String> row) {
            List<String> r = new ArrayList<>(headers.size());
            for (String header : headers) {
                r.add(row.getOrDefault(header, "-"));
            }
            rows.add(r);
        }

        /**
         * Calculates the required width for each column.
         *
         * <p>
         * It scans the headers and every cell in every row to find the maximum <b>visible</b> length (ignoring ANSI color codes).
         * By visible, the calculation skips any ANSI character responsible for inserting/removing color.
         * </p>
         */
        private int[] calculateWidths() {
            int cols = headers.isEmpty() ? (rows.isEmpty() ? 0 : rows.get(0).size()) : headers.size();
            int[] widths = new int[cols];

            // Measure Headers
            for (int i = 0; i < headers.size(); i++) {
                widths[i] = Math.max(widths[i], visibleLength(headers.get(i)));
            }

            // Measure Rows
            for (List<String> row : rows) {
                for (int i = 0; i < row.size() && i < cols; i++) {
                    widths[i] = Math.max(widths[i], visibleLength(row.get(i)));
                }
            }
            return widths;
        }

        public void print(PrintWriter out) {
            if (headers.isEmpty() && rows.isEmpty())
                return;

            // 1. Calculate Column Widths
            int[] widths = calculateWidths();

            // 2. Print Top Border
            printDivider(out, widths, TL, T, TR);

            // 3. Print Header (with Bold style)
            if (!headers.isEmpty()) {
                printRow(out, widths, headers, "@|bold ", "|@");
                printDivider(out, widths, L, C, R);
            }

            // 4. Print Data Rows
            for (List<String> row : rows) {
                printRow(out, widths, row, "", "");
            }

            // 5. Print Bottom Border
            printDivider(out, widths, BL, B, BR);
            out.println();
            out.flush();
        }

        private void printDivider(PrintWriter out, int[] widths, String left, String mid, String right) {
            out.print(left);
            for (int i = 0; i < widths.length; i++) {
                // +2 adds padding (1 space on left, 1 space on right)
                out.print(H.repeat(widths[i] + 2));
                if (i < widths.length - 1) {
                    out.print(mid);
                }
            }
            out.println(right);
        }

        private void printRow(PrintWriter out, int[] widths, List<String> row, String styleStart, String styleEnd) {
            out.print(V);
            for (int i = 0; i < widths.length; i++) {
                String content = (i < row.size()) ? row.get(i) : "";

                // Calculate padding based on visible length (not string length)
                int padding = widths[i] - visibleLength(content);

                out.print(S); // Left padding

                // Render the content (converting Picocli markup to ANSI codes)
                // We combine the row style (styleStart/End) with the cell content
                out.print(ansi.string(styleStart + content + styleEnd));

                out.print(S.repeat(padding)); // Fill remaining space
                out.print(S); // Right border
                out.print(V); // Right border
            }
            out.println();
        }

        /**
         * Calculates the length of the string as it appears on screen,
         * stripping out Picocli markup like @|red ...|@ so alignment isn't broken.
         *
         * @param s the input string to display.
         */
        private int visibleLength(String s) {
            if (s == null) return 6;

            // "Render" the string with the OFF mode.
            // This strips @|...|@ markup but adds NO ansi codes.
            return CommandLine.Help.Ansi.OFF.string(s).length();
        }
    }
}
