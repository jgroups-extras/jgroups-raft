package org.jgroups.blocks.raft;

import org.jgroups.Channel;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.protocols.raft.*;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides a consensus based distributed counter (similar to AtomicLong) which can be atomically updated across a cluster.
 * @author Bela Ban
 * @since  0.2
 */
public class CounterService implements StateMachine, RAFT.RoleChange {
    protected Channel  ch;
    protected RAFT     raft;
    protected Settable settable; // usually CLIENT (at the top of the stack)
    protected long     repl_timeout=20000; // timeout (ms) to wait for a majority to ack a write

    /** If true, reads can return the local counter value directly. Else, reads have to go through the leader */
    protected boolean  allow_dirty_reads=true;

    // the counters, keyed by name
    protected final Map<String,Long> counters=new HashMap<>();

    protected static enum Command {create, delete, get, set, compareAndSet, incrementAndGet, decrementAndGet, addAndGet}


    public CounterService(Channel ch) {
        setChannel(ch);
    }

    public void setChannel(Channel ch) {
        this.ch=ch;
        if((raft=RAFT.findProtocol(RAFT.class, ch.getProtocolStack().getTopProtocol(),true)) == null)
            throw new IllegalStateException("RAFT protocol must be present in configuration");
        raft.stateMachine(this);
        if((settable=RAFT.findProtocol(Settable.class, ch.getProtocolStack().getTopProtocol(),true)) == null)
            throw new IllegalStateException("Did not find a protocol implementing Settable");
        raft.addRoleListener(this);
    }

    public void           addRoleChangeListener(RAFT.RoleChange listener)  {raft.addRoleListener(listener);}
    public long           replTimeout()                 {return repl_timeout;}
    public CounterService replTimeout(long timeout)     {this.repl_timeout=timeout; return this;}
    public boolean        allowDirtyReads()             {return allow_dirty_reads;}
    public CounterService allowDirtyReads(boolean flag) {allow_dirty_reads=flag; return this;}
    public int            lastApplied()                 {return raft.lastApplied();}
    public int            commitIndex()                 {return raft.commitIndex();}
    public void           snapshot() throws Exception   {raft.snapshot();}
    public int            logSize()                     {return raft.logSizeInBytes();}

    /**
     * Returns an existing counter, or creates a new one if none exists
     * @param name Name of the counter, different counters have to have different names
     * @param initial_value The initial value of a new counter if there is no existing counter. Ignored
     * if the counter already exists
     * @return The counter implementation
     */
    public Counter getOrCreateCounter(String name, long initial_value) throws Exception {
        Object retval=invoke(Command.create, name, false, initial_value);
        if(retval instanceof Long)
            counters.put(name, (Long)retval);
        return new CounterImpl(name, this);
    }

  
    /**
     * Deletes a counter instance (on the coordinator)
     * @param name The name of the counter. No-op if the counter doesn't exist
     */
    public void deleteCounter(String name) throws Exception {
        invoke(Command.delete, name, true);
    }


    public String printCounters() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,Long> entry: counters.entrySet()) {
            sb.append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    public long get(String name) throws Exception {
        Object retval=invoke(Command.get, name, false);
        return (Long)retval;
    }

    public void set(String name, long new_value) {

    }

    public boolean compareAndSet(String name, long expect, long update) {
        return false;
    }

    public long incrementAndGet(String name) throws Exception {
        Object retval=invoke(Command.incrementAndGet, name, false);
        return (Long)retval;
    }

    public long decrementAndGet(String name) throws Exception {
        Object retval=invoke(Command.decrementAndGet, name, false);
        return (Long)retval;
    }

    public long addAndGet(String name, long delta) {
        return 0;
    }


    @Override
    public byte[] apply(byte[] data, int offset, int length) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        Command command=Command.values()[in.readByte()];
        String name=Bits.readAsciiString(in).toString();
        long v1, v2, retval;
        switch(command) {
            case create:
                v1=Bits.readLong(in);
                retval=_create(name, v1);
                return Util.objectToByteBuffer(retval);
            case delete:
                _delete(name);
                break;
            case get:
                return Util.objectToByteBuffer(_get(name));
            case set:
                break;
            case compareAndSet:
                break;
            case incrementAndGet:
                retval=_incr(name);
                return Util.objectToByteBuffer(retval);
            case decrementAndGet:
                break;
            case addAndGet:
                break;
            default:
                throw new IllegalArgumentException("command " + command + " is unknown");
        }
        return Util.objectToByteBuffer(null); // todo: remove when all branches have been implemented
    }

    @Override
    public void readContentFrom(DataInput in) throws Exception {

    }

    @Override
    public void writeContentTo(DataOutput out) throws Exception {

    }

    public void dumpLog() {
        raft.logEntries(new Log.Function() {
            @Override public boolean apply(int index, int term, byte[] command, int offset, int length) {
                StringBuilder sb=new StringBuilder().append(index).append(" (").append(term).append("): ");
                if(command == null) {
                    sb.append("<marker record>");
                    System.out.println(sb);
                    return true;
                }
                ByteArrayDataInputStream in=new ByteArrayDataInputStream(command, offset, length);
                try {
                    Command cmd=Command.values()[in.readByte()];
                    String name=Bits.readAsciiString(in).toString();
                    switch(cmd) {
                        case create:
                        case set:
                        case addAndGet:
                            sb.append(print(cmd, name, 1, in));
                            break;
                        case delete:
                        case get:
                        case incrementAndGet:
                        case decrementAndGet:
                            sb.append(print(cmd, name, 0, in));
                            break;
                        case compareAndSet:
                            sb.append(print(cmd, name, 2, in));
                            break;
                        default:
                            throw new IllegalArgumentException("command " + cmd + " is unknown");
                    }
                }
                catch(Throwable t) {
                    sb.append(t);
                }
                System.out.println(sb);
                return true;
            }
        });
    }

    @Override
    public void roleChanged(Role role) {
        System.out.println("-- changed role to " + role);
    }

    protected Object invoke(Command command, String name, boolean ignore_return_value, long ... values) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(256);
        try {
            out.writeByte(command.ordinal());
            Bits.writeAsciiString(new AsciiString(name), out);
            for(long val: values)
                Bits.writeLong(val, out);
        }
        catch(Exception ex) {
            throw new Exception("serialization failure (cmd=" + command + ", name=" + name + ")");
        }

        byte[] buf=out.buffer();
        byte[] rsp=settable.set(buf, 0, out.position(), repl_timeout, TimeUnit.MILLISECONDS);
        return ignore_return_value? null: Util.objectFromByteBuffer(rsp);
    }

    protected static String print(Command command, String name, int num_args, DataInput in) {
        StringBuilder sb=new StringBuilder(command.toString()).append("(").append(name);
        for(int i=0; i < num_args; i++) {
            try {
                long val=Bits.readLong(in);
                sb.append(", ").append(val);
            }
            catch(IOException e) {
                break;
            }
        }
        sb.append(")");
        return sb.toString();
    }

    protected long _get(String name) {
        return counters.get(name);
    }

    protected long _create(String name, long initial_value) {
        synchronized(counters) {
            Long val=counters.get(name);
            if(val != null)
                return val;
            counters.put(name, initial_value);
            return initial_value;
        }
    }

    protected long _incr(String name) {
        synchronized(counters) {
            long val=counters.get(name);
            counters.put(name, ++val);
            return val;
        }
    }

    protected void _delete(String name) {
        synchronized(counters) {
            counters.remove(name);
        }
    }

}