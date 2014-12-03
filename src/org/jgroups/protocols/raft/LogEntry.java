package org.jgroups.protocols.raft;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An element in a log. Captures the term and command to be applied to the state machine
 * @author Bela Ban
 * @since  0.1
 */
public class LogEntry implements Externalizable {
    protected int    term;     // the term of this entry
    protected final byte[] command;  // the command (interpreted by the state machine)
    protected final int    offset;   // may get removed (always 0)
    protected int    length;   // may get removed (always command.length)

    public LogEntry(int term,byte[] command,int offset,int length) {
        this.term=term;
        this.command=command;
        this.offset=offset;
        this.length=length;
    }

    public String toString() {
        return term + ": " + command.length + " bytes";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(term);
        out.write(length);
        out.write(command);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        term = in.readInt();
        length = in.readInt();
        in.read(command, 0, length);
    }
}
