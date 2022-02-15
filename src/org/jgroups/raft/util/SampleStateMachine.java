package org.jgroups.raft.util;

import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sample state machine accepting additions and subtractions
 * @author Bela Ban
 * @since  1.0.5
 */
public class SampleStateMachine implements StateMachine {
    protected final AtomicInteger counter=new AtomicInteger();
    protected final AtomicInteger additions=new AtomicInteger();
    protected final AtomicInteger subtractions=new AtomicInteger();

    public int counter()      {return counter.get();}
    public int additions()    {return additions.get();}
    public int subtractions() {return subtractions.get();}

    public byte[] apply(byte[] data, int offset, int length) throws Exception {
        int val=Bits.readInt(data, offset);
        if(val < 0)
            subtractions.incrementAndGet();
        else
            additions.incrementAndGet();
        int old_counter=counter.get();
        counter.addAndGet(val);
        byte[] retval=new byte[Integer.BYTES];
        Bits.writeInt(old_counter, retval, 0);
        return retval;
    }

    public void readContentFrom(DataInput in) throws Exception {
        int val=in.readInt();
        counter.set(val);
    }

    public void writeContentTo(DataOutput out) throws Exception {
        out.writeInt(counter.get());
    }

    public SampleStateMachine reset() {
        counter.set(0); additions.set(0); subtractions.set(0);
        return this;
    }

    public String toString() {
        return String.format("counter=%d (%d additions %d subtractions)",
                             counter.get(), additions.get(), subtractions.get());
    }
}
