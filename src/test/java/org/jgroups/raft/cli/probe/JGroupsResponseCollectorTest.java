package org.jgroups.raft.cli.probe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.PROBE_EXCEPTION_CODE;

import org.jgroups.Global;
import org.jgroups.raft.cli.exceptions.JGroupsProbeException;
import org.jgroups.util.ByteArray;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsResponseCollectorTest {

    public void testNullResponse() {
        JGroupsProbeRunner.JGroupsResponseCollector collector = new JGroupsProbeRunner.JGroupsResponseCollector();
        collector.accept(null);

        assertThat(collector.responses()).isEmpty();
    }

    public void testEmptyResponse() {
        JGroupsProbeRunner.JGroupsResponseCollector collector = new JGroupsProbeRunner.JGroupsResponseCollector();
        collector.accept(new ByteArray(new byte[0]));

        assertThat(collector.responses()).isEmpty();
    }

    public void testSimpleFormattedResponse() {
        String responseA = """
A [ip=192.168.100.106:41936, 3 mbr(s), cluster=total-order, version=5.5.3.Final-SNAPSHOT (Vorder Höhi) (java 21.0.9+10)]
response={"last-applied":1366,"node":"A","term":5}
""";
        String responseB = """
B [ip=192.168.100.106:39941, 3 mbr(s), cluster=total-order, version=5.5.3.Final-SNAPSHOT (Vorder Höhi) (java 21.0.9+10)]
response={"last-applied":1360,"node":"B","term":5}
""";
        JGroupsProbeRunner.JGroupsResponseCollector collector = new JGroupsProbeRunner.JGroupsResponseCollector();

        collector.accept(new ByteArray(responseA.getBytes(StandardCharsets.UTF_8)));

        assertThat(collector.responses()).hasSize(1);

        collector.accept(new ByteArray(responseB.getBytes(StandardCharsets.UTF_8)));

        assertThat(collector.responses()).hasSize(2);

        Iterator<ProbeResponseWriter.ProbeResponse> responses = collector.responses().iterator();

        ProbeResponseWriter.ProbeResponse prA = responses.next();
        ProbeResponseWriter.ProbeResponse prB = responses.next();

        assertThat(prA.raftId()).isEqualTo("A");
        assertThat(prA.source()).isNotNull();
        assertThat(prA.response())
                .hasSize(3)
                .containsEntry("last-applied", 1366)
                .containsEntry("node", "A")
                .containsEntry("term", 5);

        assertThat(prB.raftId()).isEqualTo("B");
        assertThat(prB.source()).isNotNull();
        assertThat(prB.response())
                .hasSize(3)
                .containsEntry("last-applied", 1360)
                .containsEntry("node", "B")
                .containsEntry("term", 5);
    }

    public void testProbeFormattedResponse() {
        String rawResponse = """
A [ip=192.168.100.106:41936, 3 mbr(s), cluster=total-order, version=5.5.3.Final-SNAPSHOT (Vorder Höhi) (java 21.0.9+10)]
RAFT={members=[A, B, C]}

""";
        JGroupsProbeRunner.JGroupsResponseCollector collector = new JGroupsProbeRunner.JGroupsResponseCollector();

        collector.accept(new ByteArray(rawResponse.getBytes(StandardCharsets.UTF_8)));

        assertThat(collector.responses()).hasSize(1);
        ProbeResponseWriter.ProbeResponse response = collector.responses().iterator().next();
        assertThat(response.raftId()).isEqualTo("A");
        assertThat(response.source()).isNotNull();
        assertThat(response.response())
                .hasSize(1)
                .containsOnly(Map.entry("content", "RAFT={members=[A, B, C]}"));
    }

    public void testOnlyHeader() {
        String raw = "A [ip=192.168.100.106:41936, 3 mbr(s), cluster=total-order, version=5.5.3.Final-SNAPSHOT (Vorder Höhi) (java 21.0.9+10)]";
        JGroupsProbeRunner.JGroupsResponseCollector collector = new JGroupsProbeRunner.JGroupsResponseCollector();

        collector.accept(new ByteArray(raw.getBytes(StandardCharsets.UTF_8)));

        assertThat(collector.responses()).hasSize(1);
        ProbeResponseWriter.ProbeResponse response = collector.responses().iterator().next();
        assertThat(response.raftId()).isEqualTo("A");
        assertThat(response.source()).isNotNull();
        assertThat(response.response()).isEmpty();
    }

    public void testMalformedString() {
        String raw = "not really a response";
        JGroupsProbeRunner.JGroupsResponseCollector collector = new JGroupsProbeRunner.JGroupsResponseCollector();

        assertThatThrownBy(() -> collector.accept(new ByteArray(raw.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(JGroupsProbeException.class)
                .isInstanceOfSatisfying(JGroupsProbeException.class, jpe -> assertThat(jpe.exitCode()).isEqualTo(PROBE_EXCEPTION_CODE))
                .cause().hasMessageContaining(raw);

        assertThat(collector.responses()).isEmpty();
    }
}
