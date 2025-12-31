package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Tests {@link org.jgroups.protocols.raft.LogEntries}
 * @author Bela Ban
 * @since  1.0.8
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class LogEntriesTest {
    protected LogEntries entries;

    @BeforeMethod protected void init() {
        entries=new LogEntries();
    }

    public void testEmpty() throws IOException, ClassNotFoundException {
        assertThat(entries).hasSize(0);
        LogEntries tmp=marshalAndUnmarshal(entries);
        assertThat(tmp).hasSize(0);
    }

    public void testAdd() throws IOException, ClassNotFoundException {
        LogEntry le1=new LogEntry(20, new byte[25]);
        LogEntry le2=new LogEntry(22, new byte[10], 0, 10, true);
        entries.add(le1);
        assertThat(entries).hasSize(1);
        entries.add(le2);
        assertThat(entries).hasSize(2);
        LogEntries e=marshalAndUnmarshal(entries);
        assertThat(e).hasSize(2);
    }

    public void testIterator() throws IOException, ClassNotFoundException {
        testAdd();
        Iterator<LogEntry> it=entries.iterator();
        LogEntry le=it.next();
        assertThat(le.term()).isEqualTo(20);
        assertThat(le.command()).hasSize(25);
        le=it.next();
        assertThat(le.term()).isEqualTo(22);
        assertThat(le.command()).hasSize(10);
        assertThat(le.internal()).isTrue();
        assertThat(it.hasNext()).isFalse();
        entries.clear();
        assertThat(entries).isEmpty();
    }

    public void testToArray() throws IOException, ClassNotFoundException {
        testAdd();
        LogEntry[] arr=entries.toArray();
        assertThat(arr).hasSize(2);
        assertThat(arr[0].term()).isEqualTo(20);
        assertThat(arr[0].command()).hasSize(25);
        assertThat(arr[1].term()).isEqualTo(22);
        assertThat(arr[1].command()).hasSize(10);
        assertThat(arr[1].internal()).isTrue();
    }

    public void testTotalSize() throws IOException, ClassNotFoundException {
        testAdd();
        long total_size=entries.totalSize();
        assertThat(total_size).isEqualTo(35);
    }


    protected static LogEntries marshalAndUnmarshal(LogEntries le) throws IOException, ClassNotFoundException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(64);
        int expected_size=le.serializedSize();
        le.writeTo(out);
        assertThat(out.position()).isEqualTo(expected_size);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());
        LogEntries ret=new LogEntries();
        ret.readFrom(in);
        return ret;
    }
}
