package org.jgroups.protocols.raft;

import java.io.InputStream;
import java.io.OutputStream;

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
     * @throws Exception Thrown on deserialization or other failure
     */
    void       apply(byte[] data, int offset, int length) throws Exception;  // tbd: should we use NIO ByteBuffers ?

    /**
     * Reads the contents of the state machine from an input stream. This can be the case when an InstallSnapshot RPC
     * is used to bootstrap a new node, or a node that's lagging far behind.<p/>
     * The parsing depends on the concrete state machine implementation, but the idea is that the stream is a sequence
     * of commands, each of which can be passed to {@link #apply(byte[],int,int)}.<p/>
     * The state machine may need to block modifications until the contents have been set (unless e.g. copy-on-write
     * is used)<p/>
     * The state machine implementation may need to remove all contents before populating itself from the stream.
     * @param in The input stream
     */
    void       readContentFrom(InputStream in);

    /**
     * Writes the contents of the state machine to an output stream. This is typically called on the leader to
     * provide state to a new node, or a node that's lagging far behind.<p/>
     * Updates to the state machine may need to be put on hold while the state is written to the output stream.
     * @param out The output stream
     */
    void       writeContentTo(OutputStream out); // -> dumps state (to ByteBuffer, output stream ?)
}
