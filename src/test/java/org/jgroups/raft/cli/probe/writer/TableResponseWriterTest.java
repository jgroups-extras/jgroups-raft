package org.jgroups.raft.cli.probe.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.cli.probe.writer.TableResponseWriter.DEFAULT_ROOT_TABLE;
import static org.jgroups.raft.cli.probe.writer.TableResponseWriter.TableWriter.BL;
import static org.jgroups.raft.cli.probe.writer.TableResponseWriter.TableWriter.BR;
import static org.jgroups.raft.cli.probe.writer.TableResponseWriter.TableWriter.L;
import static org.jgroups.raft.cli.probe.writer.TableResponseWriter.TableWriter.R;
import static org.jgroups.raft.cli.probe.writer.TableResponseWriter.TableWriter.TL;
import static org.jgroups.raft.cli.probe.writer.TableResponseWriter.TableWriter.TR;

import org.jgroups.Global;
import org.jgroups.raft.cli.probe.ProbeResponseWriter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class TableResponseWriterTest {

    public void testEmptyResponse() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new TableResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.TABLE);

        prw.accept(Collections.emptyList());

        assertThat(sw.toString()).contains("No responses received");
    }

    public void testTableWriter() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new TableResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.TABLE);

        String raftId = UUID.randomUUID().toString();
        ProbeResponseWriter.ProbeResponse response = new ProbeResponseWriter.ProbeResponse(raftId, null, Map.of("key", "value"));

        prw.accept(List.of(response));

        String output = sw.toString();
        assertThat(output)
                // The table has all 4 corners
                .contains(TL, TR, BL, BR)
                // Since it has only one line, there is only 1 left and right symbols.
                .containsOnlyOnce(L)
                .containsOnlyOnce(R)

                // Now the column names of the table
                .containsOnlyOnce(TableResponseWriter.RAFT_ID_KEY)
                .containsOnlyOnce(TableResponseWriter.ADDRESS_KEY)
                .containsOnlyOnce("key")

                // Now a single row with all the values.
                .containsOnlyOnce(raftId)
                .containsOnlyOnce(" - ")
                .containsOnlyOnce("value");
    }

    public void testMultipleTables() {
        StringWriter sw = new StringWriter();
        ProbeResponseWriter prw = new TableResponseWriter(new PrintWriter(sw));

        assertThat(prw.format()).isEqualTo(ProbeResponseWriterFormat.TABLE);

        String raftId = UUID.randomUUID().toString();
        Map<String, String> table1 = Map.of("t1a", "v11", "t1b", "v12");
        Map<String, String> table2 = Map.of("t2a", "v21", "t2b", "v22");
        ProbeResponseWriter.ProbeResponse response = new ProbeResponseWriter.ProbeResponse(raftId, null, Map.of("key", "value", "table1", table1, "table2", table2));

        prw.accept(List.of(response));

        String output = sw.toString();
        assertThat(output)
                .containsOnlyOnce(String.format("[%s]", DEFAULT_ROOT_TABLE))
                .containsOnlyOnce("[table1]")
                .containsOnlyOnce("[table2]");

        assertTableContents(raftId, table1, output);
        assertTableContents(raftId, table2, output);
    }

    private void assertTableContents(String raftId, Map<String, String> content, String output) {
        assertThat(output).contains(TableResponseWriter.RAFT_ID_KEY, TableResponseWriter.ADDRESS_KEY, raftId);
        for (Map.Entry<String, String> entry : content.entrySet()) {
            assertThat(output)
                    .containsOnlyOnce(entry.getKey())
                    .containsOnlyOnce(entry.getValue());
        }
    }
}
