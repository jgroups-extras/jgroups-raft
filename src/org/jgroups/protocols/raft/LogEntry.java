package org.jgroups.protocols.raft;

/**
 * An element in a log. Captures the term and command to be applied to the state machine
 * @author Bela Ban
 * @since  0.1
 */
public class LogEntry {
    protected final int    term;     // the term of this entry
    protected final byte[] command;  // the command (interpreted by the state machine)
    protected final int    offset;   // may get removed (always 0)
    protected final int    length;   // may get removed (always command.length)

    public LogEntry(int term,byte[] command,int offset,int length) {
        this.term=term;
        this.command=command;
        this.offset=offset;
        this.length=length;
    }

    public String toString() {
        return term + ": " + command.length + " bytes";
    }
}
