package org.jgroups.raft.cli.probe.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.cli.probe.writer.TextResponseWriter.HEADER_TEMPLATE;
import static org.jgroups.raft.cli.probe.writer.TextResponseWriter.KEY_VALUE_TEMPLATE;

import org.jgroups.Global;
import org.jgroups.raft.cli.probe.ProbeResponseWriter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class RepeatableResponseWriterTest {

    public void testRepeatableWriter() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter original = new TextResponseWriter(new PrintWriter(sw));
        ProbeResponseWriter prw = new RepeatableResponseWriter(original);

        assertThat(prw.format()).isEqualTo(original.format());

        ProbeResponseWriter.ProbeResponse response = new ProbeResponseWriter.ProbeResponse("A", null, Map.of("k1", "v1"));
        prw.accept(List.of(response));

        String output = sw.toString();
        assertThat(output)
                .containsOnlyOnce(String.format(HEADER_TEMPLATE, "A", null))
                .containsOnlyOnce(String.format(KEY_VALUE_TEMPLATE, "k1", "v1"));

        // Now, trigger the method again.
        // Since we are running in the test, ANSI is not enabled
        prw.accept(List.of(response));
        assertThat(RepeatableResponseWriter.ANSI.enabled()).isFalse();

        String extra = sw.toString();
        assertThat(extra)
                .containsSequence(output, removeLastLine(output));
    }

    private String removeLastLine(String original) {
        String[] parts = original.split(System.lineSeparator());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            sb.append(parts[i]).append(System.lineSeparator());
        }
        return sb.toString();
    }
}
