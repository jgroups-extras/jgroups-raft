package org.jgroups.raft.cli.probe.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.cli.probe.writer.TextResponseWriter.HEADER_TEMPLATE;
import static org.jgroups.raft.cli.probe.writer.TextResponseWriter.KEY_VALUE_TEMPLATE;

import org.jgroups.Global;
import org.jgroups.raft.cli.probe.ProbeResponseWriter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class TextResponseWriterTest {

    public void testEmptyResponse() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new TextResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.TEXT);

        prw.accept(Collections.emptyList());
        assertThat(sw.toString()).isEmpty();
    }

    public void testSingleResponseFormat() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new TextResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.TEXT);

        ProbeResponseWriter.ProbeResponse response = new ProbeResponseWriter.ProbeResponse("A", null, Map.of("k1", "v1"));
        prw.accept(List.of(response));

        String output = sw.toString();
        assertThat(output)
                .containsOnlyOnce(String.format(HEADER_TEMPLATE, "A", null))
                .containsOnlyOnce(String.format(KEY_VALUE_TEMPLATE, "k1", "v1"));
    }

    public void testNestedResponse() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new TextResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.TEXT);

        Map<String, Object> r1Out = new LinkedHashMap<>();
        r1Out.put("k1", "v1");
        r1Out.put("m", Map.of("k2", "v2"));
        ProbeResponseWriter.ProbeResponse r1 = new ProbeResponseWriter.ProbeResponse("A", null, r1Out);

        Map<String, Object> r2Out = new LinkedHashMap<>();
        r2Out.put("k1", "v1");
        r2Out.put("l", List.of(1, 2, 3));
        ProbeResponseWriter.ProbeResponse r2 = new ProbeResponseWriter.ProbeResponse("B", null, r2Out);
        prw.accept(List.of(r1, r2));

        String output = sw.toString();
        String expected = """
--- A (null) ---
k1=v1
m.k2=v2

--- B (null) ---
k1=v1
l[0]=1
l[1]=2
l[2]=3

""";
        assertThat(output).isEqualTo(expected);
    }
}
