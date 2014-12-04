package org.jgroups.protocols.raft;

import java.io.Serializable;

/**
 * An element in a log. Captures the term and command to be applied to the state machine
 * @author Bela Ban
 * @since  0.1
 */
public class LogEntry implements Serializable {

    protected int    term;     // the term of this entry
    protected byte[] command;  // the command (interpreted by the state machine)
    protected transient int    offset;   // may get removed (always 0)
    protected int    length;   // may get removed (always command.length)

    public LogEntry(int term,byte[] command,int offset,int length) {
        this.term=term;
        this.command=command;
        this.offset=offset;
        this.length=length;
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("Term: ").append(term).append(" Command Bytes: { ");
        for (byte b: command) {
            str.append(b);
            str.append(" ");
        }
        str.append("}");
        return str.toString();
    }
}
