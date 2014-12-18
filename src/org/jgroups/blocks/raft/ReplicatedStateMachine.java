package org.jgroups.blocks.raft;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Settable;
import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.InputStream;
import java.io.OutputStream;
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
    protected RAFT                     raft;
    protected Settable                 settable;
    protected JChannel                 ch;
    protected long                     repl_timeout=20000; // timeout (ms) to wait for a majority to ack a write
    protected final List<Notification> listeners=new ArrayList<>();

    // Hashmap for the contents. Doesn't need to be reentrant, as updates will be applied sequentially
    protected final Map<K,V>           map=new HashMap<K,V>();

    protected static final byte        PUT    = 1;
    protected static final byte        REMOVE = 2;


    public ReplicatedStateMachine(JChannel ch) {
        this.ch=ch;
        if((raft=RAFT.findProtocol(RAFT.class, ch.getProtocolStack().getTopProtocol(),true)) == null)
            throw new IllegalStateException("RAFT protocol must be present in configuration");
        raft.stateMachine(this);
        if((settable=RAFT.findProtocol(Settable.class, ch.getProtocolStack().getTopProtocol(),true)) == null)
            throw new IllegalStateException("Did not find a protocol implementing Settable");
    }

    public      ReplicatedStateMachine timeout(long timeout)       {this.repl_timeout=timeout; return this;}
    public void addRoleChangeListener(RAFT.RoleChange listener)    {raft.addRoleListener(listener);}
    public void addNotificationListener(Notification n)            {if(n != null) listeners.add(n);}
    public void removeNotificationListener(Notification n)         {listeners.remove(n);}
    public void removeRoleChangeListener(RAFT.RoleChange listener) {raft.remRoleListener(listener);}
    public int  lastApplied()                                      {return raft.lastApplied();}
    public int  commitIndex()                                      {return raft.commitIndex();}

    public void dumpLog() {
        raft.logEntries(new Log.Function() {
            @Override public boolean apply(int index, int term, byte[] command, int offset, int length) {
                StringBuilder sb=new StringBuilder().append(index).append(" (").append(term).append("): ");
                ByteArrayDataInputStream in=new ByteArrayDataInputStream(command, offset, length);
                try {
                    byte type=in.readByte();
                    switch(type) {
                        case PUT:
                            K key=(K)Util.objectFromStream(in);
                            V val=(V)Util.objectFromStream(in);
                            sb.append("put(").append(key).append(", ").append(val).append(")");
                            break;
                        case REMOVE:
                            key=(K)Util.objectFromStream(in);
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
                return true;
            }
        });
    }

    public void loadFromLog() throws Exception {
        raft.initStateMachineFromLog(false);
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
     * returning stale data. In the next version, we'll look into returning a value based on consensus.
     *
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


    ///////////////////////////////////////// StateMachine callbacks /////////////////////////////////////

    @Override public byte[] apply(byte[] data, int offset, int length) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        byte command=in.readByte();
        switch(command) {
            case PUT:
                K key=(K)Util.objectFromStream(in);
                V val=(V)Util.objectFromStream(in);
                V old_val=map.put(key, val);
                notifyPut(key, val, old_val);
                return old_val == null? null : Util.objectToByteBuffer(old_val);
            case REMOVE:
                key=(K)Util.objectFromStream(in);
                old_val=map.remove(key);
                notifyRemove(key, old_val);
                return old_val == null? null : Util.objectToByteBuffer(old_val);
            default:
                throw new IllegalArgumentException("command " + command + " is unknown");
        }
    }

    @Override public void readContentFrom(InputStream in) {

    }

    @Override public void writeContentTo(OutputStream out) {

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
        byte[] rsp=settable.set(buf, 0, out.position(), repl_timeout, TimeUnit.MILLISECONDS);
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

    public static interface Notification<K,V> {
        void put(K key, V val, V old_val);
        void remove(K key, V old_val);
    }
}
