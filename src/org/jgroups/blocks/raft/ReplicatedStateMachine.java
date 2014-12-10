package org.jgroups.blocks.raft;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A key-value store replicating its contents with RAFT via consensus
 * @author Bela Ban
 * @since  0.1
 */
public class ReplicatedStateMachine<K,V> implements StateMachine {
    protected RAFT     raft;
    protected JChannel ch;

    // Hashmap for the contents. Doesn't need to be reentrant, as updates will be applied sequentially
    protected final Map<K,V> map=new HashMap<K,V>();
    protected static final byte SET = 1;
    protected static final byte REM = 2;


    public ReplicatedStateMachine(JChannel ch) {
        this.ch=ch;
        if((raft=RAFT.findProtocol(RAFT.class,ch.getProtocolStack().getTopProtocol(),true)) == null)
            throw new IllegalStateException("RAFT protocol must be present in configuration");
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
        return invoke(SET, key, val, false);
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
    public void remove(K key) throws Exception {
        invoke(REM, key, null, true);
    }


    ///////////////////////////////////////// StateMachine callbacks /////////////////////////////////////

    @Override public void apply(byte[] data, int offset, int length) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        byte command=in.readByte();
        switch(command) {
            case SET:
                K key=(K)Util.objectFromStream(in);
                V val=(V)Util.objectFromStream(in);
                map.put(key, val); // todo: synchronization ?
                break;
            case REM:
                key=(K)Util.objectFromStream(in);
                map.remove(key);
                break;
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
        byte[] rsp=raft.set(buf, 0, out.position());
        return ignore_return_value? null: (V)Util.objectFromByteBuffer(rsp);
    }
}
