package org.jgroups.raft.cli.probe.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.cli.probe.ProbeResponseWriter.ADDRESS_KEY;
import static org.jgroups.raft.cli.probe.ProbeResponseWriter.RAFT_ID_KEY;
import static org.jgroups.raft.cli.probe.ProbeResponseWriter.RESPONSE_KEY;

import org.jgroups.Global;
import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.util.internal.Json;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JsonResponseWriterTest {

    public void testEmptyResponse() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new JsonResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.JSON);

        prw.accept(Collections.emptyList());
        assertThat(sw.toString()).isEqualTo("[]");
    }

    public void testNodesWithEmptyPayload() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new JsonResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.JSON);

        ProbeResponseWriter.ProbeResponse r1 = new ProbeResponseWriter.ProbeResponse("A", null, Collections.emptyMap());
        ProbeResponseWriter.ProbeResponse r2 = new ProbeResponseWriter.ProbeResponse("B", null, Collections.emptyMap());

        prw.accept(List.of(r1, r2));

        assertThat(sw.toString())
                .startsWith("[{")
                .endsWith("}]")
                .contains("},{")
                .contains("\"response\":{}", "\"address\":null")
                .containsOnlyOnce("\"raft-id\":\"A\"")
                .containsOnlyOnce("\"raft-id\":\"B\"");
    }

    public void testSingleResponse() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new JsonResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.JSON);

        ProbeResponseWriter.ProbeResponse response = new ProbeResponseWriter.ProbeResponse("A", null, Map.of("k1", "v1", "k2", 42, "k3", true));

        prw.accept(List.of(response));

        String output = sw.toString();
        assertThat(output)
                .startsWith("[{")
                .endsWith("}]");

        // Let's remove the array delimiters and use the inner JSON parser.
        output = output.substring(1);
        output = output.substring(0, output.length() - 1);
        Map<String, Object> json = Json.fromJson(output);
        assertThat(json)
                .containsEntry(RAFT_ID_KEY, response.raftId())
                .containsEntry(ADDRESS_KEY, response.source())
                .satisfies(j -> assertThat(response.response()).isEqualTo(j.get(RESPONSE_KEY)));
    }
}
