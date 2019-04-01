package org.jgroups.raft.blocks;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.InternalCommand;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.StateMachine;
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
    protected final List<Notification<K,V>> listeners=new ArrayList<>();

    // Hashmap for the contents. Doesn't need to be reentrant, as updates will be applied sequentially
    protected final Map<K,V>           map=new HashMap<>();

    protected static final byte        PUT    = 1;
    protected static final byte        REMOVE = 2;


    public ReplicatedStateMachine(JChannel ch) {
        this.ch=ch;
        this.raft=new RaftHandle(this.ch, this);
    }

    public      ReplicatedStateMachine<K,V> timeout(long timeout)       {this.repl_timeout=timeout; return this;}
    public void addRoleChangeListener(RAFT.RoleChange listener)    {raft.addRoleListener(listener);}
    public void addNotificationListener(Notification<K,V> n)            {if(n != null) listeners.add(n);}
    public void removeNotificationListener(Notification<K,V> n)         {listeners.remove(n);}
    public void removeRoleChangeListener(RAFT.RoleChange listener) {raft.removeRoleListener(listener);}
    public int  lastApplied()                                      {return raft.lastApplied();}
    public int  commitIndex()                                      {return raft.commitIndex();}
    public JChannel channel()                                      {return ch;}
    public void snapshot() throws Exception                        {if(raft != null) raft.snapshot();}
    public int  logSize()                                          {return raft != null? raft.logSizeInBytes() : 0;}
    public String raftId()                                         {return raft.raftId();}
    public ReplicatedStateMachine<K,V> raftId(String id)           {raft.raftId(id); return this;}

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
                    sb.append("[internal] ").append(cmd).append("\n");
                }
                catch(Exception ex) {
                    sb.append("[failure reading internal cmd] ").append(ex).append("\n");
                }
                System.out.println(sb);
                return;
            }
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(entry.command(), entry.offset(), entry.length());
            try {
                byte type=in.readByte();
                switch(type) {
                    case PUT:
                        K key=Util.objectFromStream(in);
                        V val=Util.objectFromStream(in);
                        sb.append("put(").append(key).append(", ").append(val).append(")");
                        break;
                    case REMOVE:
                        key=Util.objectFromStream(in);
                        sb.append("remove(").append(key).append(")");
                        break;
                    default:
                        sb.append("type " + type + " is unknown");
                }
            }
            catch(Throwable t) {
                sb.append(t);
            }
            System.out.println(sb);
        });
    }

    @Override
    public boolean equals(Object obj) {
        return map.equals(((ReplicatedStateMachine)obj).map);
    }

    @Override
    public int hashCode() {
    	return map.hashCode();
    }


    /**
     * Adds a key value pair to the state machine. The data is not added directly, but sent to the RAFT leader and only
     * added to the hashmap after the change has been committed (by majority decision). The actual change will be
     * applied with callback {@link #apply(byte[],int,int)}.
     *
     * @param key The key to be added.
     * @param val The value to be added
     * @return Null, or the previous value associated with key (if present)
     */
    public V put(K key, V val) throws Exception {
        return invoke(PUT, key, val, false);
    }

    /**
     * Returns the value for a given key. Currently, the hashmap is accessed directly to return the value, possibly
     * returning stale data. In the next version, we'll look into returning a value based on consensus, or returning
     * the value from the leader (configurable).
     * @param key The key
     * @return The value associated with key (might be stale)
     */
    public V get(K key) {
        return map.get(key);
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
    public int size() {return map.size();}


    ///////////////////////////////////////// StateMachine callbacks /////////////////////////////////////

    @Override public byte[] apply(byte[] data, int offset, int length) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        byte command=in.readByte();
        switch(command) {
            case PUT:
                K key=Util.objectFromStream(in);
                V val=Util.objectFromStream(in);
                V old_val=map.put(key, val);
                notifyPut(key, val, old_val);
                return old_val == null? null : Util.objectToByteBuffer(old_val);
            case REMOVE:
                key=Util.objectFromStream(in);
                old_val=map.remove(key);
                notifyRemove(key, old_val);
                return old_val == null? null : Util.objectToByteBuffer(old_val);
            default:
                throw new IllegalArgumentException("command " + command + " is unknown");
        }
    }

    @Override public void readContentFrom(DataInput in) throws Exception {
        int size=Bits.readInt(in);
        for(int i=0; i < size; i++) {
            K key=Util.objectFromStream(in);
            V val=Util.objectFromStream(in);
            map.put(key, val);
        }
    }

    @Override public void writeContentTo(DataOutput out) throws Exception {
        int size=map.size();
        Bits.writeInt(size, out);
        for(Map.Entry<K,V> entry: map.entrySet()) {
            Util.objectToStream(entry.getKey(), out);
            Util.objectToStream(entry.getValue(), out);
        }
    }

    ///////////////////////////////////// End of StateMachine callbacks ///////////////////////////////////


    public String toString() {
        return map.toString();
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
        return ignore_return_value? null: (V)Util.objectFromByteBuffer(rsp);
    }

    protected void notifyPut(K key, V val, V old_val) {
        for(Notification<K,V> n: listeners) {
            try {n.put(key, val, old_val);}catch(Throwable t) {}
        }
    }

    protected void notifyRemove(K key, V old_val) {
        for(Notification<K,V> n: listeners) {
            try {n.remove(key, old_val);}catch(Throwable t) {}
        }
    }

    public interface Notification<K,V> {
        void put(K key, V val, V old_val);
        void remove(K key, V old_val);
    }
}
