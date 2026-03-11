package org.jgroups.raft.cli.probe;

import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.PROBE_EXCEPTION_CODE;

import org.jgroups.Address;
import org.jgroups.raft.cli.exceptions.JGroupsProbeException;
import org.jgroups.raft.internal.probe.RaftProtocolProbe;
import org.jgroups.raft.util.internal.Json;
import org.jgroups.stack.IpAddress;
import org.jgroups.tests.Probe;
import org.jgroups.util.ByteArray;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default implementation of {@link ProbeRunner} that delegates to the JGroups {@link Probe} utility.
 *
 * <p>
 * This class handles the complexity of:
 * <ol>
 *   <li>Configuring the JGroups Probe client (UDP/TCP, Timeouts).</li>
 *   <li>Sending the request to the specified cluster members.</li>
 *   <li>Capturing raw byte responses and parsing them into structured {@link ProbeResponseWriter.ProbeResponse} objects.</li>
 * </ol>
 *
 * @author José Bolina
 * @since 2.0
 */
final class JGroupsProbeRunner implements ProbeRunner {
    private static final String PROBE_HELP_ARGUMENT = "-help";

    static final ProbeRunner INSTANCE = new JGroupsProbeRunner();

    private JGroupsProbeRunner() { }

    @Override
    public void run(ProbeArguments arguments, ProbeResponseWriter handler) throws Throwable {
        Probe probe = new Probe();
        probe.verbose(arguments.verbose());

        JGroupsResponseCollector collector = null;
        if (handler != null) {
            collector = new JGroupsResponseCollector();

            // Configure the callback to parse raw packet buffers coming from JGroups.
            probe.setDefaultResponseHandler(collector);
        }

        if (PROBE_HELP_ARGUMENT.equals(arguments.request())) {
            Probe.main(new String[] { PROBE_HELP_ARGUMENT });
            return;
        }

        // Trigger the network request
        // This will block until the timeout option elapses.
        probe.start(arguments.addresses(), arguments.bindAddress(), arguments.port(), 32, arguments.timeout(), arguments.request(), null, false, arguments.passcode(), arguments.udp(), arguments.tcp());

        // Hand off collected responses to the writer for display
        if (collector != null) {
            handler.accept(collector.responses());
        }
    }

    static final class JGroupsResponseCollector implements Consumer<ByteArray>, ProbeContentParser.Internal.ResponseCollector {
        // Thread-safe list to collect asynchronous responses from multiple nodes.
        // Utilizing a thread-safe implementation just to be on the safe side.
        private final List<ProbeResponseWriter.ProbeResponse> responses = new CopyOnWriteArrayList<>();

        @Override
        public void accept(ByteArray buf) {
            try {
                if (buf == null)
                    return;

                // Parse only the valid bytes (offset + length) to avoid "ghost null" characters
                ProbeContentParser.Internal.parse(buf.array(), buf.offset(), buf.length(), this);
            } catch (Exception e) {
                throw new JGroupsProbeException("Failed parsing probe response", PROBE_EXCEPTION_CODE, e);
            }
        }

        @Override
        public void accept(String raftId, Address source, Map<String, Object> response) {
            if (response != null)
                responses.add(new ProbeResponseWriter.ProbeResponse(raftId, source, response));
        }

        public Collection<ProbeResponseWriter.ProbeResponse> responses() {
            return responses;
        }
    }

    /**
     * Internal utility for parsing the raw JGroups Probe wire format.
     * <p>
     * A Probe response typically consists of:
     * <pre>
     * HEADER (Node ID, IP Address) \n BODY (JSON or Plain Text)
     * </pre>
     * </p>
     */
    private static final class ProbeContentParser {
        private static final class Internal {
            // Regex Explanation:
            // ^(\S+)        -> Group 1: The ID (Starts at beginning, non-whitespace characters)
            // .* -> Skip arbitrary text
            // ip=([^,\]]+)  -> Group 2: The Address (Capture chars until a comma or closing bracket)
            private static final Pattern HEADER_PATTERN = Pattern.compile("^(\\S+).*ip=([^,\\]]+)");
            private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

            interface ResponseCollector {
                void accept(String raftId, Address source, Map<String, Object> response);
            }

            /**
             * Parses a single probe response packet.
             *
             * @param datum   The raw byte array from the network packet.
             * @param offset  The start index of the valid data.
             * @param length  The length of the valid data.
             * @param handler The callback to invoke with the parsed data.
             */
            public static void parse(byte[] datum, int offset, int length, ResponseCollector handler) throws Exception {
                if (datum == null || length == 0) {
                    handler.accept(null, null, null);
                    return;
                }

                // 1. Find the newline separator between Header and Body
                int splitIndex = -1;
                for (int i = offset; i < offset + length; i++) {
                    if (datum[i] == '\n') {
                        splitIndex = i;
                        break;
                    }
                }

                // Handle case where there is no body (just header)
                int headerLength = (splitIndex == -1) ? length : splitIndex;

                // 2. Parse Header
                // We only decode the bytes up to the newline
                String header = new String(datum, offset, headerLength - offset, StandardCharsets.UTF_8).trim();
                Matcher matcher = HEADER_PATTERN.matcher(header);
                if (!matcher.find()) {
                    String response = new String(datum, offset, length, StandardCharsets.UTF_8);
                    throw new IllegalStateException("Invalid probe response: " + response);
                }

                String raftId = matcher.group(1);
                Address address = new IpAddress(matcher.group(2));

                // 3. Parse Body
                ByteBuffer content;
                if (splitIndex != -1 && splitIndex + 1 < offset + length) {
                    int contentStart = splitIndex + 1;
                    content = ByteBuffer.wrap(datum, contentStart, offset + length - contentStart);
                } else {
                    content = EMPTY_BUFFER;
                }

                Map<String, Object> response;
                if (content == EMPTY_BUFFER) {
                    response = Collections.emptyMap();
                } else {
                    String s = StandardCharsets.UTF_8.decode(content).toString();

                    // If the response is wrapped in our specific key, strip it and parse JSON
                    // This is implemented by the RaftProtocolProbe class to handle custom commands.
                    if (s.startsWith(RaftProtocolProbe.PROBE_RESPONSE_KEY)) {
                        s = s.substring(RaftProtocolProbe.PROBE_RESPONSE_KEY.length() + 1);
                        response = Json.fromJson(s);
                    } else {
                        // Otherwise treat as plain text.
                        // This option is retuned by "raw" probe commands.
                        // For example, running `RAFT.member`, we just display the raw string.
                        response = Map.of("content", s.trim());
                    }
                }

                if (handler != null)
                    handler.accept(raftId, address, response);
            }
        }
    }
}
