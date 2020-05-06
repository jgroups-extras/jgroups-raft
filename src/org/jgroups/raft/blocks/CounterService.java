package org.jgroups.raft.blocks;

import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.protocols.raft.InternalCommand;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.raft.RaftHandle;
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
    protected JChannel   ch;
    protected RaftHandle raft;
    protected long       repl_timeout=20000; // timeout (ms) to wait for a majority to ack a write

    /** If true, reads can return the local counter value directly. Else, reads have to go through the leader */
    protected boolean  allow_dirty_reads=true;

    // keys: counter names, values: counter values
    protected final Map<String,Long> counters=new HashMap<>();

    protected enum Command {create, delete, get, set, compareAndSet, incrementAndGet, decrementAndGet, addAndGet}


    public CounterService(JChannel ch) {
        setChannel(ch);
    }

    public void setChannel(JChannel ch) {
        this.ch=ch;
        this.raft=new RaftHandle(this.ch, this);
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
    public String         raftId()                      {return raft.raftId();}
    public CounterService raftId(String id)             {raft.raftId(id); return this;}


    /**
     * Returns an instance of the counter. This is local operation which never fails and don't create a record in the
     * log. For cluster-wide operation call getOrCreateCounter(name, initial_value).
     * @param name Name of the counter, different counters have to have different names
     * @return The counter instance
     */
    public Counter counter(String name) {
        return new CounterImpl(name, this);
    }


    /**
     * Returns an existing counter, or creates a new one if none exists. This is a cluster-wide operation which would
     * fail if no leader is elected.
     * @param name Name of the counter, different counters have to have different names
     * @param initial_value The initial value of a new counter if there is no existing counter. Ignored
     * if the counter already exists
     * @return The counter implementation
     */
    public Counter getOrCreateCounter(String name, long initial_value) throws Exception {
        invoke(Command.create, name, false, initial_value);
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
        return counters.entrySet()
          .stream().collect(StringBuilder::new,
                            (sb,entry) -> sb.append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n"),
                            (l,r) -> {}).toString();
    }


    public long get(String name) throws Exception {
        Object retval=allow_dirty_reads? _get(name) : invoke(Command.get, name, false);
        return (long)retval;
    }

    public void set(String name, long new_value) throws Exception {
        invoke(Command.set, name, true, new_value);
    }

    public boolean compareAndSet(String name, long expect, long update) throws Exception {
        Object retval=invoke(Command.compareAndSet, name, false, expect, update);
        return (boolean)retval;
    }

    public long incrementAndGet(String name) throws Exception {
        Object retval=invoke(Command.incrementAndGet, name, false);
        return (long)retval;
    }

    public long decrementAndGet(String name) throws Exception {
        Object retval=invoke(Command.decrementAndGet, name, false);
        return (long)retval;
    }

    public long addAndGet(String name, long delta) throws Exception {
        Object retval=invoke(Command.addAndGet, name, false, delta);
        return (long)retval;
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
                retval=_get(name);
                return Util.objectToByteBuffer(retval);
            case set:
                v1=Bits.readLong(in);
                _set(name, v1);
                break;
            case compareAndSet:
                v1=Bits.readLong(in);
                v2=Bits.readLong(in);
                boolean success=_cas(name, v1, v2);
                return Util.objectToByteBuffer(success);
            case incrementAndGet:
                retval=_add(name, +1L);
                return Util.objectToByteBuffer(retval);
            case decrementAndGet:
                retval=_add(name, -1L);
                return Util.objectToByteBuffer(retval);
            case addAndGet:
                v1=Bits.readLong(in);
                retval=_add(name, v1);
                return Util.objectToByteBuffer(retval);
            default:
                throw new IllegalArgumentException("command " + command + " is unknown");
        }
        return Util.objectToByteBuffer(null);
    }


    @Override
    public void writeContentTo(DataOutput out) throws Exception {
        synchronized(counters) {
            int size=counters.size();
            out.writeInt(size);
            for(Map.Entry<String,Long> entry: counters.entrySet()) {
                AsciiString name=new AsciiString(entry.getKey());
                Long value=entry.getValue();
                Bits.writeAsciiString(name, out);
                Bits.writeLong(value, out);
            }
        }
    }

    @Override
    public void readContentFrom(DataInput in) throws Exception {
        int size=in.readInt();
        for(int i=0; i < size; i++) {
            AsciiString name=Bits.readAsciiString(in);
            Long value=Bits.readLong(in);
            counters.put(name.toString(), value);
        }
    }

    public void dumpLog() {
        raft.logEntries((entry, index) -> {
            StringBuilder sb=new StringBuilder().append(index).append(" (").append(entry.term()).append("): ");
            if(entry.command() == null) {
                sb.append("<marker record>");
                System.out.println(sb);
                return;
            }
            if(entry.internal()) {
                try {
                    InternalCommand cmd=Util.streamableFromByteBuffer(InternalCommand.class,
                                                                      entry.command(), entry.offset(), entry.length());
                    sb.append("[internal] ").append(cmd);
                }
                catch(Exception ex) {
                    sb.append("[failure reading internal cmd] ").append(ex);
                }
                System.out.println(sb);
                return;
            }
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(entry.command(), entry.offset(), entry.length());
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
        byte[] rsp=raft.set(buf, 0, out.position(), repl_timeout, TimeUnit.MILLISECONDS);
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

    protected long _create(String name, long initial_value) {
        synchronized(counters) {
            Long val=counters.get(name);
            if(val != null)
                return val;
            counters.put(name, initial_value);
            return initial_value;
        }
    }

    protected void _delete(String name) {
        synchronized(counters) {
            counters.remove(name);
        }
    }


    protected long _get(String name) {
        synchronized(counters) {
            Long retval=counters.get(name);
            return retval != null? (long)retval : 0;
        }
    }

    protected void _set(String name, long new_val) {
        synchronized(counters) {
            counters.put(name, new_val);
        }
    }

    protected boolean _cas(String name, long expected, long value) {
        synchronized(counters) {
            Long existing_value=counters.get(name);
            if(existing_value == null) return false;
            if(existing_value == expected) {
                counters.put(name, value);
                return true;
            }
            return false;
        }
    }

    protected long _add(String name, long delta) {
        synchronized(counters) {
            Long val=counters.get(name);
            if(val == null)
                val=(long)0;
            counters.put(name, val+delta);
            return val+delta;
        }
    }



}