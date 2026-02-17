package org.jgroups.raft.util;

import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.raft.StateMachine;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sample state machine accepting additions and subtractions
 * @author Bela Ban
 * @since  1.0.5
 */
public class CounterStateMachine implements StateMachine {
    protected final AtomicInteger counter=new AtomicInteger();
    protected final AtomicInteger additions=new AtomicInteger();
    protected final AtomicInteger subtractions=new AtomicInteger();

    public int counter()      {return counter.get();}
    public int additions()    {return additions.get();}
    public int subtractions() {return subtractions.get();}

    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
        int val=Bits.readInt(data, offset);
        if(val < 0)
            subtractions.incrementAndGet();
        else if (val > 0)
            additions.incrementAndGet();
        int old_counter=counter.get();
        counter.addAndGet(val);
        if(!serialize_response)
            return null;
        byte[] retval=new byte[Integer.BYTES];
        Bits.writeInt(old_counter, retval, 0);
        return retval;
    }

    public static String readAndDumpSnapshot(DataInput in) {
        try {
            int num=in.readInt();
            return String.valueOf(num);
        }
        catch(Exception ex) {
            return null;
        }
    }

    public static String reader(LogEntry le) {
        byte[] buf=le.command();
        int offset=le.offset();
        int val=Bits.readInt(buf, offset);
        return String.valueOf(val);
    }

    public void readContentFrom(DataInput in) {
        try {
            int val=in.readInt();
            counter.set(val);
        } catch(Exception ignore) { }
    }

    public void writeContentTo(DataOutput out) throws Exception {
        out.writeInt(counter.get());
    }

    public CounterStateMachine reset() {
        counter.set(0); additions.set(0); subtractions.set(0);
        return this;
    }

    public String toString() {
        return String.format("counter=%d (%d additions %d subtractions)",
                             counter.get(), additions.get(), subtractions.get());
    }
}
