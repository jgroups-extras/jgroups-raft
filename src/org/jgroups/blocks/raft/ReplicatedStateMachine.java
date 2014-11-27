package org.jgroups.blocks.raft;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.StateMachine;

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
    protected RAFT raft;
    protected JChannel ch;

    // Hashmap for the contents. Doesn't need to be reentrant, as updates will be applied sequentially
    protected final Map<K,V> map=new HashMap<K,V>();


    public ReplicatedStateMachine(JChannel ch) {
        this.ch=ch;
        raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
        if(raft == null)
            throw new IllegalStateException("RAFT protocol must be present in configuration");
    }

    /**
     * Adds a key value pair to the state machine. The data is not added directly, but sent to the RAFT leader and only
     * added to the hashmap after the change has been committed (by majority decision). The actual change will be
     * applied with callback {@link #set(java.nio.ByteBuffer)}.
     *
     * @param key The key to be added.
     * @param value The value to be added
     * @return Null, or the previous value associated with key (if present)
     */
    public V put(K key, V value) {
        // todo: serialize the command into a byte[] buffer and call raft.set(buffer), or possibly use an event
        return null;
    }

    /**
     * Returns the value for a given key. Currently, the hashmap is accessed directly to return the value, possibly
     * returning stale data. In the next version, we'll look into returning a value based on consensus.
     *
     * @param key The key
     * @return The value associated with key
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
    public void remove(K key) {
        // todo: serialize the command into a byte[] buffer and call raft.set(buffer), or possibly use an event
    }


    ///////////////////////////////////////// StateMachine callbacks /////////////////////////////////////

    @Override public void apply(byte[] data, int offset, int length) {
        // todo: parse the byte[] buffer into a command (e.g. put(), remove() or clear()) and apply the command
    }

    @Override public void readContentFrom(InputStream in) {

    }

    @Override public void writeContentTo(OutputStream out) {

    }

    ///////////////////////////////////// End of StateMachine callbacks ///////////////////////////////////

}
