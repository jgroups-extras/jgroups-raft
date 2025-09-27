package org.jgroups.protocols.raft;

import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

/**
 * List of {@link LogEntry} elements, provides efficient serialization. Used mainly in {@link AppendEntriesRequest}
 * messages. Note that this class is unsynchronized, as it is intended to be used by a single thread.
 * <br/>
 * Format: <pre>| num-elements | log-entry0 | log-entry1 ... | log-entryN | </pre>
 * @author Bela Ban
 * @since  1.0.8
 */
public class LogEntries implements SizeStreamable, Iterable<LogEntry> {
    protected ArrayList<LogEntry> entries;


    public LogEntries add(LogEntry ... log_entries) {
        if(entries == null)
            entries=new ArrayList<>(log_entries.length);
        else
            entries.ensureCapacity(log_entries.length);
        for(LogEntry le: log_entries)
            entries.add(Objects.requireNonNull(le));
        return this;
    }

    public static LogEntries create(LogEntry... entries) {
        return new LogEntries().add(entries);
    }

    public LogEntries clear() {
        if(entries != null)
            entries.clear();
        return this;
    }

    public Iterator<LogEntry> iterator() {
        if(entries == null)
            entries=new ArrayList<>();
        return entries.iterator();
    }

    public int size() {
        return entries != null? entries.size() : 0;
    }

    // will be removed as soon as Log.append(term, LogEntry... entries) has been changed to append(term, LogEntries e)
    public LogEntry[] toArray() {
        if(entries == null)
            return new LogEntry[0];
        LogEntry[] ret=new LogEntry[size()];
        int index=0;
        for(LogEntry le: entries)
            ret[index++]=le;
        return ret;
    }

    public long totalSize() {
        final ArrayList<LogEntry> tmp=this.entries;
        if(tmp == null)
            return 0;
        long length=0;
        for(int i=0, size=tmp.size(); i < size; i++) {
            LogEntry entry=tmp.get(i);
            if(entry != null)
                length+=entry.length;
        }
        return length;
    }

    public int serializedSize() {
        int size=size();
        int retval=Bits.size(size);
        if(size > 0) {
            for(LogEntry le: entries)
                retval+=le.serializedSize();
        }
        return retval;
    }

    public void writeTo(DataOutput out) throws IOException {
        int size=size();
        Bits.writeIntCompressed(size, out);
        if(size > 0) {
            for(LogEntry le: entries)
                le.writeTo(out);
        }
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size=Bits.readIntCompressed(in);
        if(size > 0) {
            entries=new ArrayList<>(size);
            for(int i=0; i < size; i++) {
                LogEntry le=new LogEntry();
                le.readFrom(in);
                entries.add(le);
            }
        }
    }

    public String toString() {
        return String.format("%d entries", size());
    }



}
