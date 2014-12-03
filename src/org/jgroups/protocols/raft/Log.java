package org.jgroups.protocols.raft;

import org.jgroups.Address;

/**
 * The interface for a (persistent) log.
 * @author Bela Ban
 * @since  0.1
 */
public interface Log {
    /** Returns the current term */
    int currentTerm();

    /** Sets the current term */
    Log currentTerm(int new_term);

    /** Returns the address of the candidate that this node voted for in the current term */
    Address votedFor();

    /** Sets the address of the member this node voted for in the current term. Only invoked once per term */
    Log votedFor(Address member);

    /** Returns the index of the first entry */
    int first();

    /** Returns the current commit index. (May get removed as the RAFT paper has this as in-memory attribute) */
    int commitIndex();

    /** Returns the index of the last applied append operation (May get removed as this should be in-memory) */
    int lastApplied();

    /** Returns the total number of entries */ // needed ?
    int size();

    /**
     * Appends one or more entries to the log.<p/>
     * If the entry at prev_index doesn't match prev_term (<code>log[prev_index].term != prev_term</code>),
     * an AppendResult of false (including the first index of the non-matching term) is returned.
     * Else an AppendResult with the last index written is returned.<p/>
     * If there are entries at prev_index+1, they will get overwritten.
     * @param prev_index The previous index
     * @param prev_term The term of the entry at the previous index
     * @param entries One of more entries
     * @return An AppendResult
     */
    AppendResult append(int prev_index, int prev_term, LogEntry[] entries);


    /**
     * Applies function to all elements of the log between start_index and end_index
     * @param function The function to be applied
     * @param start_index The start index. If smaller than first(), first() will be used
     * @param end_index The end index. If greater than commitIndex(), commitIndex() will be used
     */
    void forEach(Function function, int start_index, int end_index);

    /** Applies a function to all elements between first() and commitIndex() */
    void forEach(Function function);

    interface Function {
        /**
         * The function to be applied to log entries in {@link #forEach(org.jgroups.protocols.raft.Log.Function)}
         * @param index The index of the entry
         * @param term The term of the entry
         * @param command The command buffer
         * @param offset The offset into the comand buffer
         * @param length The length of the command buffer
         * @return True if the iteration should continue, false if it should terminate
         */
        boolean apply(int index, int term, byte[] command, int offset, int length);
    }
}
