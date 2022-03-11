package org.jgroups.tests;

import org.jgroups.protocols.raft.StateMachine;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * @author Bela Ban
 * @since  1.0.5
 */
public class DummyStateMachine implements StateMachine {
    public byte[] apply(byte[] data, int offset, int length) throws Exception {return new byte[0];}
    public void readContentFrom(DataInput in) throws Exception {}
    public void writeContentTo(DataOutput out) throws Exception {}
}
