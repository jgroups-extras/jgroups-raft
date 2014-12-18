package org.jgroups.protocols.raft;

import org.jgroups.util.CompletableFuture;
import org.jgroups.util.Consumer;

import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @since  0.1
 */
public interface Settable {
    byte[] set(byte[] buf, int offset, int length) throws Exception;
    byte[] set(byte[] buf, int offset, int length, long timeout, TimeUnit unit) throws Exception;
    CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Consumer<byte[]> completion_handler);
}
