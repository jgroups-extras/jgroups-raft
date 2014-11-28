package org.jgroups.protocols.raft;

/**
 * @author Bela Ban
 * @since  0.1
 */
public interface Settable {
    void set(byte[] buf, int offset, int length);
}
