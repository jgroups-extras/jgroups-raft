package org.jgroups.raft;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * Interface of a state machine which stores data in memory. Committed log entries are applied to the state machine.
 * @author Bela Ban
 * @since  0.1
 */
public interface StateMachine {
    /**
     * Applies a command to the state machine. The contents of the byte[] buffer are interpreted by the state machine.
     * The command could for example be a set(), remove() or clear() command.
     * @param data The byte[] buffer
     * @param offset The offset at which the data starts
     * @param length The length of the data
     * @param serialize_response If true, serialize and return the response, else return null
     * @return A serialized response value, or null (e.g. if the method returned void)
     * @throws Exception Thrown on deserialization or other failure
     */
    // todo: use ByteBuffers?
    byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception;

    /**
     * Reads the contents of the state machine from an input stream. This can be the case when an InstallSnapshot RPC
     * is used to bootstrap a new node, or a node that's lagging far behind.<p/>
     * The parsing depends on the concrete state machine implementation, but the idea is that the stream is a sequence
     * of commands, each of which can be passed to {@link #apply(byte[], int, int, boolean)}.<p/>
     * The state machine may need to block modifications until the contents have been set (unless e.g. copy-on-write
     * is used)<p/>
     * The state machine implementation may need to remove all contents before populating itself from the stream.
     * @param in The input stream
     */
    void   readContentFrom(DataInput in) throws Exception;

    /**
     * Writes the contents of the state machine to an output stream. This is typically called on the leader to
     * provide state to a new node, or a node that's lagging far behind.<p/>
     * Updates to the state machine may need to be put on hold while the state is written to the output stream.
     * @param out The output stream
     */
    void   writeContentTo(DataOutput out) throws Exception;
}
