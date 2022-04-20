package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
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
        assert entries.size() == 0;
        LogEntries tmp=marshalAndUnmarshal(entries);
        assert tmp.size() == 0;
    }

    public void testAdd() throws IOException, ClassNotFoundException {
        LogEntry le1=new LogEntry(20, new byte[25]);
        LogEntry le2=new LogEntry(22, new byte[10], 0, 10, true);
        entries.add(le1);
        assert entries.size() == 1;
        entries.add(le2);
        assert entries.size() == 2;
        LogEntries e=marshalAndUnmarshal(entries);
        assert e.size() == 2;
    }

    public void testIterator() throws IOException, ClassNotFoundException {
        testAdd();
        Iterator<LogEntry> it=entries.iterator();
        LogEntry le=it.next();
        assert le.term() == 20;
        assert le.command().length == 25;
        le=it.next();
        assert le.term() == 22;
        assert le.command().length == 10;
        assert le.internal();
        assert !it.hasNext();
        entries.clear();
        assert entries.size() == 0;
    }

    public void testToArray() throws IOException, ClassNotFoundException {
        testAdd();
        LogEntry[] arr=entries.toArray();
        assert arr.length == 2;
        assert arr[0].term() == 20;
        assert arr[0].command().length == 25;
        assert arr[1].term() == 22;
        assert arr[1].command().length == 10;
        assert arr[1].internal();
    }



    protected static LogEntries marshalAndUnmarshal(LogEntries le) throws IOException, ClassNotFoundException {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(64);
        int expected_size=le.serializedSize();
        le.writeTo(out);
        assert out.position() == expected_size;
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(out.buffer(), 0, out.position());
        LogEntries ret=new LogEntries();
        ret.readFrom(in);
        return ret;
    }
}
