package org.jgroups.protocols.raft;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Interface of a state machine which stores data in memory. Committed log entries are applied to the state machine.
 * @author Bela Ban
 * @since  0.1
 */
public interface StateMachine {
    /** Applies a command to the state machine */
    void set(ByteBuffer data);

    /** Applies multiple commands to the state machine */
    void set(ByteBuffer[] data);



    // OR

    void write(Object key, Object value); // ?

    ByteBuffer get(ByteBuffer key);


    void readContentFrom(InputStream in);
    void writeContentTo(OutputStream out); // -> dumps state (to ByteBuffer, output stream ?)
}
