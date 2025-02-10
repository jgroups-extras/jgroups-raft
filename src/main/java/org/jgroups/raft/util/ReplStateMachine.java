package org.jgroups.raft.util;

import org.jgroups.raft.StateMachine;
import org.jgroups.util.Bits;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * Dummy replicated hashmap state machine
 * @author Bela Ban
 * @since  1.0.5
 */
public class ReplStateMachine<K,V> implements StateMachine {
    protected final Map<K,V> map=new HashMap<>();
    public static final int PUT    = 1;
    public static final int REMOVE = 2;

    @Override
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(data, offset, length);
        byte command=in.readByte();
        switch(command) {
            case PUT:
                K key=Util.objectFromStream(in);
                V val=Util.objectFromStream(in);
                V old_val;
                synchronized(map) {
                    old_val=map.put(key, val);
                }
                return old_val == null? null : serialize_response? Util.objectToByteBuffer(old_val) : null;
            case REMOVE:
                key=Util.objectFromStream(in);
                synchronized(map) {
                    old_val=map.remove(key);
                }
                return old_val == null? null : serialize_response? Util.objectToByteBuffer(old_val) : null;
            default:
                throw new IllegalArgumentException("command " + command + " is unknown");
        }
    }

    @Override public void readContentFrom(DataInput in) throws Exception {
        int size=Bits.readIntCompressed(in);
        Map<K,V> tmp=new HashMap<>(size);
        for(int i=0; i < size; i++) {
            K key=Util.objectFromStream(in);
            V val=Util.objectFromStream(in);
            tmp.put(key, val);
        }
        synchronized(map) {
            map.putAll(tmp);
        }
    }

    @Override public void writeContentTo(DataOutput out) throws Exception {
        synchronized(map) {
            int size=map.size();
            Bits.writeIntCompressed(size, out);
            for(Map.Entry<K,V> entry: map.entrySet()) {
                Util.objectToStream(entry.getKey(), out);
                Util.objectToStream(entry.getValue(), out);
            }
        }
    }
}
