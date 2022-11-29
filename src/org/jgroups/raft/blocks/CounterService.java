package org.jgroups.raft.blocks;

import net.jcip.annotations.GuardedBy;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.CounterFunction;
import org.jgroups.blocks.atomic.CounterView;
import org.jgroups.protocols.raft.InternalCommand;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.Options;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Provides a consensus based distributed counter (similar to AtomicLong) which can be atomically updated across a cluster.
 * @author Bela Ban
 * @since  0.2
 */
public class CounterService implements StateMachine, RAFT.RoleChange {
    protected RaftHandle             raft;
    protected long                   repl_timeout=20000; // timeout (ms) to wait for a majority to ack a write

    /** If true, reads can return the local counter value directly. Else, reads have to go through the leader */
    protected boolean                allow_dirty_reads=true;

    // keys: counter names, values: counter values
    @GuardedBy("counters")
    protected final Map<String,Long> counters=new HashMap<>();

    protected enum Command {create, delete, get, set, addAndGet, compareAndSwap, updateFunction}


    public CounterService(JChannel ch) {
        setChannel(ch);
    }

    public void setChannel(JChannel ch) {
        raft=new RaftHandle(ch, this).addRoleListener(this);
    }

    public JChannel       channel()                     {return raft.channel();}
    public void           addRoleChangeListener(RAFT.RoleChange listener)  {raft.addRoleListener(listener);}
    public long           replTimeout()                 {return repl_timeout;}
    public CounterService replTimeout(long timeout)     {this.repl_timeout=timeout; return this;}
    public boolean        allowDirtyReads()             {return allow_dirty_reads;}
    public CounterService allowDirtyReads(boolean flag) {allow_dirty_reads=flag; return this;}
    public long           lastApplied()                 {return raft.lastApplied();}
    public long           commitIndex()                 {return raft.commitIndex();}
    public void           snapshot() throws Exception   {raft.snapshot();}
    public long           logSize()                     {return raft.logSize();}
    public String         raftId()                      {return raft.raftId();}
    public CounterService raftId(String id)             {raft.raftId(id); return this;}



    /**
     * Returns an existing counter, or creates a new one if none exists. This is a cluster-wide operation which would
     * fail if no leader is elected.
     * @param name Name of the counter, different counters have to have different names
     * @param initial_value The initial value of a new counter if there is no existing counter. Ignored
     * if the counter already exists
     * @return The counter implementation
     */
    public RaftSyncCounter getOrCreateCounter(String name, long initial_value) throws Exception {
        return CompletableFutures.join(getOrCreateAsyncCounter(name, initial_value)).sync();
    }


    /**
     * Deletes a counter instance (on the coordinator)
     * @param name The name of the counter. No-op if the counter doesn't exist
     */
    public void deleteCounter(String name) throws Exception {
        CompletableFutures.join(deleteCounterAsync(name));
    }

    /**
     * Deletes a counter instance.
     *
     * @param name The name of the counter. No-op if the counter doesn't exist
     * @return Returns a {@link CompletionStage} which is completed when the majority reach consensus.
     */
    public CompletionStage<Void> deleteCounterAsync(String name) {
        AsciiString counterName = new AsciiString(name);
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(Bits.size(counterName) + Global.BYTE_SIZE);
        try {
            writeCommandAndName(out, Command.delete.ordinal(), counterName);
            return setAsyncWithTimeout(out, Options.DEFAULT_OPTIONS).thenApply(CompletableFutures.toVoidFunction());
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }


    public String printCounters() {
        synchronized (counters) {
            return counters.entrySet().stream().map(e -> String.format("%s = %d", e.getKey(), e.getValue()))
                    .collect(Collectors.joining("\n"));
        }
    }


    @Override
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        Command command=Command.values()[in.readByte()];
        String name=Bits.readAsciiString(in).toString();
        long val;
        Object retval=null;
        switch(command) {
            case create:
                val=Bits.readLongCompressed(in);
                retval=_create(name, val);
                break;
            case delete:
                _delete(name);
                break;
            case get:
                retval=_get(name);
                break;
            case set:
                val=Bits.readLongCompressed(in);
                _set(name, val);
                break;
            case addAndGet:
                val=Bits.readLongCompressed(in);
                retval=_add(name, val);
                break;
            case compareAndSwap:
                retval=_compareAndSwap(name, Bits.readLongCompressed(in), Bits.readLongCompressed(in));
                break;
            case updateFunction:
                retval=_update(name, Util.readGenericStreamable(in));
                break;
            default:
                throw new IllegalArgumentException("command " + command + " is unknown");
        }
        return serialize_response? Util.objectToByteBuffer(retval) : null;
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
                Bits.writeLongCompressed(value, out);
            }
        }
    }

    @Override
    public void readContentFrom(DataInput in) throws Exception {
        synchronized (counters) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                AsciiString name = Bits.readAsciiString(in);
                Long value = Bits.readLongCompressed(in);
                counters.put(name.toString(), value);
            }
        }
    }

    public static String readAndDumpSnapshot(DataInput in) {
        try {
            int size=in.readInt();
            StringBuilder sb=new StringBuilder();
            for(int i=0; i < size; i++) {
                AsciiString name=Bits.readAsciiString(in);
                Long value=Bits.readLongCompressed(in);
                sb.append(name).append(": ").append(value);
            }
            return sb.toString();
        }
        catch(Exception ex) {
            return null;
        }
    }

    public void dumpLog() {
        raft.logEntries((entry, index) -> {
            StringBuilder sb=new StringBuilder().append(index).append(" (").append(entry.term()).append("): ");
            sb.append(dumpLogEntry(entry));
            System.out.println(sb);
        });
    }

    public static String dumpLogEntry(LogEntry e) {
        if(e.command() == null)
            return "<marker record>";

        StringBuilder sb=new StringBuilder();
        if(e.internal()) {
            try {
                InternalCommand cmd=Util.streamableFromByteBuffer(InternalCommand.class, e.command(), e.offset(), e.length());
                sb.append("[internal] ").append(cmd);
            }
            catch(Exception ex) {
                sb.append("[failure reading internal cmd] ").append(ex);
            }
            return sb.toString();
        }
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(e.command(), e.offset(), e.length());
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
                case compareAndSwap:
                    sb.append(print(cmd, name, 2, in));
                    break;
                default:
                    throw new IllegalArgumentException("command " + cmd + " is unknown");
            }
        }
        catch(Throwable t) {
            sb.append(t);
        }
        return sb.toString();
    }

    @Override
    public void roleChanged(Role role) {
        System.out.println("-- changed role to " + role);
    }

    public String toString() {
        return printCounters();
    }

    /*
     Async operations
     */

    /**
     * Returns an {@link RaftAsyncCounter} instance of the counter.
     * <p>
     * This is local operation, and it does not create the counter in the raft log.
     *
     * @param name Name of the counter, different counters have to have different names.
     * @return The {@link RaftAsyncCounter} instance
     */
    public RaftAsyncCounter asyncCounter(String name) {
        return new AsyncCounterImpl(this, name);
    }

    /**
     * Returns an existing counter, or creates a new one if none exists.
     * <p>
     * This is a cluster-wide operation which would fail if no leader is elected.
     *
     * @param name         Name of the counter, different counters have to have different names
     * @param initialValue The initial value of a new counter if there is no existing counter. Ignored if the counter
     *                     already exists
     * @return The {@link RaftAsyncCounter} implementation.
     */
    public CompletionStage<RaftAsyncCounter> getOrCreateAsyncCounter(String name, long initialValue) {
        synchronized (counters) {
            if (counters.containsKey(name)) {
                return CompletableFuture.completedFuture(asyncCounter(name));
            }
        }
        return invokeAsync(Command.create, new AsciiString(name), initialValue)
              .thenApply(__ -> asyncCounter(name));
    }

    protected CompletionStage<Long> asyncGet(AsciiString name) {
        return invokeAsyncAndGet(Command.get, name, Options.DEFAULT_OPTIONS); // ignoring the return value doesn't make sense!
    }

    protected CompletionStage<Void> asyncSet(AsciiString name, long value) {
        return invokeAsync(Command.set, name, value);
    }

    protected CompletionStage<Long> asyncAddAndGet(AsciiString name, long delta, Options opts) {
        return delta == 0 ?
              asyncGet(name) :
              invokeAsyncAddAndGet(name, delta, opts);
    }

    protected CompletionStage<Long> asyncCompareAndSwap(AsciiString name, long expected, long value, Options opts) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(Bits.size(name) + Global.BYTE_SIZE + Bits.size(expected) + Bits.size(value));
        try {
            writeCommandAndName(out, Command.compareAndSwap.ordinal(), name);
            Bits.writeLongCompressed(expected, out);
            Bits.writeLongCompressed(value, out);
            return setAsyncWithTimeout(out, opts).thenApply(CounterService::readLong);
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    protected <T extends Streamable> CompletionStage<T> asyncUpdate(AsciiString name, CounterFunction<T> function, Options options) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(Bits.size(name) + Global.BYTE_SIZE + 128);
        try {
            writeCommandAndName(out, Command.updateFunction.ordinal(), name);
            Util.writeGenericStreamable(function, out);
            return setAsyncWithTimeout(out, options).thenApply(CounterService::safeStreamableFromBytes);
        } catch (Throwable throwable) {
            return CompletableFuture.failedFuture(throwable);
        }
    }

    protected CompletionStage<Long> invokeAsyncAndGet(Command command, AsciiString name, Options opts) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(Bits.size(name) + Global.BYTE_SIZE);
        try {
            writeCommandAndName(out, command.ordinal(), name);
            return setAsyncWithTimeout(out, opts).thenApply(CounterService::readLong);
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    protected CompletionStage<Long> invokeAsyncAddAndGet(AsciiString name, long arg, Options opts) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(Bits.size(name) + Global.BYTE_SIZE + Bits.size(arg));
        try {
            writeCommandAndName(out, Command.addAndGet.ordinal(), name);
            Bits.writeLongCompressed(arg, out);
            return setAsyncWithTimeout(out, opts).thenApply(CounterService::readLong);
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    protected CompletionStage<Void> invokeAsync(Command command, AsciiString name, long arg) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(Bits.size(name) + Global.BYTE_SIZE + Bits.size(arg));
        try {
            writeCommandAndName(out, command.ordinal(), name);
            Bits.writeLongCompressed(arg, out);
            return setAsyncWithTimeout(out, Options.DEFAULT_OPTIONS).thenApply(CompletableFutures.toVoidFunction());
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    private static void writeCommandAndName(ByteArrayDataOutputStream out, int command, AsciiString name) throws IOException {
        out.writeByte(command);
        Bits.writeAsciiString(name, out);
    }

    private CompletionStage<byte[]> setAsyncWithTimeout(ByteArrayDataOutputStream out, Options opts) throws Exception {
        return raft.setAsync(out.buffer(), 0, out.position(), opts)
              .orTimeout(repl_timeout, TimeUnit.MILLISECONDS);
    }

    private static Long readLong(byte[] rsp) {
        try {
            return Util.objectFromByteBuffer(rsp);
        } catch (IOException | ClassNotFoundException e) {
            throw CompletableFutures.wrapAsCompletionException(e);
        }
    }

    protected static String print(Command command, String name, int num_args, DataInput in) {
        StringBuilder sb=new StringBuilder(command.toString()).append("(").append(name);
        for(int i=0; i < num_args; i++) {
            try {
                long val=Bits.readLongCompressed(in);
                sb.append(", ").append(val);
            }
            catch(IOException ignored) {
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


    protected long _add(String name, long delta) {
        synchronized(counters) {
            Long val=counters.get(name);
            if(val == null)
                val=(long)0;
            counters.put(name, val+delta);
            return val+delta;
        }
    }

    protected long _compareAndSwap(String name, long expected, long value) {
        synchronized (counters) {
            Long existing = counters.get(name);
            if (existing == null) {
                // TODO is it ok to return 0?
                return expected == 0 ? 1: 0;
            }
            if (existing == expected) {
                counters.put(name, value);
            }
            return existing;
        }
    }

    protected <T extends Streamable> T _update(String name, CounterFunction<T> function) {
        synchronized (counters) {
            long original = counters.getOrDefault(name, 0L);
            CounterViewImpl view = new CounterViewImpl(original);
            T result = function.apply(view);
            counters.put(name, view.value);
            return result;
        }
    }

    private static <T extends Streamable> T safeStreamableFromBytes(byte[] bytes) {
        if (bytes == null) {
            // ignore return value
            return null;
        }
        try {
            return Util.objectFromByteBuffer(bytes);
        } catch (IOException | ClassNotFoundException e) {
            throw CompletableFutures.wrapAsCompletionException(e);
        }
    }

    private static class CounterViewImpl implements CounterView {

        long value;

        CounterViewImpl(long value) {
            this.value = value;
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public void set(long value) {
            this.value = value;
        }
    }
}