package org.jgroups.raft.blocks;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.InternalCommand;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.RaftHandle;
import org.jgroups.util.Bits;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A key-value store replicating its contents with RAFT via consensus
 * @author Bela Ban
 * @since  0.1
 */
public class ReplicatedStateMachine<K,V> implements StateMachine {
    protected JChannel                 ch;
    protected RaftHandle               raft;
    protected long                     repl_timeout=20000; // timeout (ms) to wait for a majority to ack a write
    // If true, reads are served locally without going through RAFT.
    protected boolean                  allow_dirty_reads=false;
    protected ClassLoader              class_loader=null;
    protected final List<Notification<K,V>> listeners=new ArrayList<>();

    // Hashmap for the contents
    protected final Map<K,V>           map=new HashMap<>();

    protected static final byte        PUT    = 1;
    protected static final byte        REMOVE = 2;
    protected static final byte        GET    = 3;


    public ReplicatedStateMachine(JChannel ch) {
        this.ch=ch;
        this.raft=new RaftHandle(this.ch, this);
    }

    public ReplicatedStateMachine<K,V> timeout(long timeout)       {this.repl_timeout=timeout; return this;}
    public long timeout()                                          {return repl_timeout;}
    public ReplicatedStateMachine<K,V> allowDirtyReads(boolean f)  {this.allow_dirty_reads=f; return this;}
    public boolean allowDirtyReads()                               {return allow_dirty_reads;}
    public void addRoleChangeListener(RAFT.RoleChange listener)    {raft.addRoleListener(listener);}
    public void addNotificationListener(Notification<K,V> n)       {if(n != null) listeners.add(n);}
    public void removeNotificationListener(Notification<K,V> n)    {listeners.remove(n);}
    public void removeRoleChangeListener(RAFT.RoleChange listener) {raft.removeRoleListener(listener);}
    public long lastApplied()                                      {return raft.lastApplied();}
    public long commitIndex()                                      {return raft.commitIndex();}
    public JChannel channel()                                      {return ch;}
    public void snapshot() throws Exception                        {if(raft != null) raft.snapshot();}
    public long logSize()                                          {return raft != null? raft.logSize() : 0;}
    public String raftId()                                         {return raft.raftId();}
    public ReplicatedStateMachine<K,V> raftId(String id)           {raft.raftId(id); return this;}

    public ReplicatedStateMachine<K, V> useClassLoader(ClassLoader classLoader) {
        this.class_loader = classLoader;
        return this;
    }

    public ClassLoader classLoaderInUse(ClassLoader classLoader) {
        return classLoader;
    }

    public String dumpLog() {
        StringBuilder sb=new StringBuilder();

        raft.logEntries((entry, index) -> {
            sb.append(index).append(" (").append(entry.term()).append("): ");
            if(entry.command() == null) {
                sb.append("<marker record>\n");
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
                sb.append("\n");
                return;
            }
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(entry.command(), entry.offset(), entry.length());
            try {
                byte type=in.readByte();
                switch(type) {
                    case PUT:
                        K key=Util.objectFromStream(in, class_loader);
                        V val=Util.objectFromStream(in, class_loader);
                        sb.append("put(").append(key).append(", ").append(val).append(")");
                        break;
                    case REMOVE:
                        key=Util.objectFromStream(in, class_loader);
                        sb.append("remove(").append(key).append(")");
                        break;
                    case GET:
                        key=Util.objectFromStream(in, class_loader);
                        sb.append("get(").append(key).append(")");
                        break;
                    default:
                        sb.append("type ").append(type).append(" is unknown");
                }
            }
            catch(Throwable t) {
                sb.append(t);
            }
            sb.append("\n");
        });
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) { // why is this method needed?
    	if(this == other) {
    		return true;
    	}
    	if(other == null) {
    		return false;
    	}
    	if(other.getClass() != getClass()) {
    		return false;
    	}
    	synchronized(map) {
            return map.equals(((ReplicatedStateMachine<?,?>)other).map);
        }
    }

    @Override
    public int hashCode() { // why is this method needed?
        synchronized(map) {
            return map.hashCode();
        }
    }


    /**
     * Adds a key value pair to the state machine. The data is not added directly, but sent to the RAFT leader and only
     * added to the hashmap after the change has been committed (by majority decision). The actual change will be
     * applied with callback {@link StateMachine#apply(byte[], int, int, boolean)}.
     *
     * @param key The key to be added.
     * @param val The value to be added
     * @return Null, or the previous value associated with key (if present)
     */
    public V put(K key, V val) throws Exception {
        return invoke(PUT, key, val, false);
    }

    /**
     * Returns the value for a given key.
     * <p>
     * When {@link #allow_dirty_reads} is set, the local value is returned, possibly stale and violating
     * linearizability. Otherwise, the request is sent through RAFT and returns a consistent value.
     *
     * @param key The key
     * @return The value associated with key (staleness configurable via {@link #allow_dirty_reads})
     */
    public V get(K key) throws Exception {
        if (allow_dirty_reads) {
            synchronized(map) {
                return map.get(key);
            }
        }

        return invoke(GET, key, null, false);
    }

    /**
     * Removes a key-value pair from the state machine. The data is not removed directly from the hashmap, but an
     * update is sent via RAFT and the actual removal from the hashmap is only done when that change has been committed.
     *
     * @param key The key to be removed
     */
    public V remove(K key) throws Exception {
        return invoke(REMOVE, key, null, true);
    }

    /** Returns the number of elements in the RSM */
    public int size() {
        synchronized(map) {
            return map.size();
        }
    }


    ///////////////////////////////////////// StateMachine callbacks /////////////////////////////////////

    @Override public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        byte command=in.readByte();
        switch(command) {
            case PUT:
                K key=Util.objectFromStream(in, class_loader);
                V val=Util.objectFromStream(in, class_loader);
                V old_val;
                synchronized(map) {
                    old_val=map.put(key, val);
                }
                notifyPut(key, val, old_val);
                return old_val == null? null : serialize_response? Util.objectToByteBuffer(old_val) : null;
            case REMOVE:
                key=Util.objectFromStream(in, class_loader);
                synchronized(map) {
                    old_val=map.remove(key);
                }
                notifyRemove(key, old_val);
                return old_val == null? null : serialize_response? Util.objectToByteBuffer(old_val) : null;
            case GET:
                key=Util.objectFromStream(in, class_loader);
                synchronized(map) {
                    val=map.get(key);
                }
                notifyGet(key, val);
                return val == null? null : serialize_response? Util.objectToByteBuffer(val) : null;
            default:
                throw new IllegalArgumentException("command " + command + " is unknown");
        }
    }

    @Override public void readContentFrom(DataInput in) throws Exception {
        int size=Bits.readIntCompressed(in);
        Map<K,V> tmp=new HashMap<>(size);
        for(int i=0; i < size; i++) {
            K key=Util.objectFromStream(in, class_loader);
            V val=Util.objectFromStream(in, class_loader);
            tmp.put(key, val);
        }
        synchronized(map) {
            map.clear();
            map.putAll(tmp);
        }
    }

    @Override public void writeContentTo(DataOutput out) throws Exception {
        synchronized(map) {
            int size=map.size();
            Bits.writeIntCompressed(size, out);
            for(Map.Entry<K,V> entry : map.entrySet()) {
                Util.objectToStream(entry.getKey(), out);
                Util.objectToStream(entry.getValue(), out);
            }
        }
    }

    ///////////////////////////////////// End of StateMachine callbacks ///////////////////////////////////


    public String toString() {
        synchronized(map) {
            return map.toString();
        }
    }

    protected V invoke(byte command, K key, V val, boolean ignore_return_value) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(256);
        try {
            out.writeByte(command);
            Util.objectToStream(key, out);
            if(val != null)
                Util.objectToStream(val, out);
        }
        catch(Exception ex) {
            throw new Exception("serialization failure (key=" + key + ", val=" + val + ")", ex);
        }

        byte[] buf=out.buffer();
        byte[] rsp=raft.set(buf, 0, out.position(), repl_timeout, TimeUnit.MILLISECONDS);
        return ignore_return_value || rsp == null ? null: Util.objectFromByteBuffer(rsp, 0, rsp.length, class_loader);
    }

    protected void notifyPut(K key, V val, V old_val) {
        for(Notification<K,V> n: listeners) {
            try {n.put(key, val, old_val);}catch(Throwable ignored) {}
        }
    }

    protected void notifyRemove(K key, V old_val) {
        for(Notification<K,V> n: listeners) {
            try {n.remove(key, old_val);}catch(Throwable ignored) {}
        }
    }

    protected void notifyGet(K key, V val) {
        for (Notification<K, V> n : listeners) {
            try {n.get(key, val);}catch(Throwable ignored) {}
        }
    }

    public interface Notification<K,V> {
        void put(K key, V val, V old_val);
        void remove(K key, V old_val);
        void get(K key, V val);
    }
}
