package org.jgroups.raft;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Bela Ban
 * @since  1.0.5
 */
public class DummyStateMachine implements StateMachine {
    public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
        return serialize_response? new byte[0] : null;
    }
    public void readContentFrom(DataInput in) throws Exception {}
    public void writeContentTo(DataOutput out) throws Exception {}
}
