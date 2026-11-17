package org.jgroups.protocols.raft;

import org.jgroups.raft.exceptions.JGroupsRaftException;
import org.jgroups.util.Bits;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

/**
 * Carries a failure from the node that processed a request back to the node that submitted it.
 *
 * <p>
 * Failures travel as a description, never as a serialized exception object: any exception can be described, the
 * receiver does not need the exception class, and no arbitrary object is deserialized from the network.
 * </p>
 *
 * <p>
 * Failures that callers act on keep their type on the receiver. Any other failure is received as a
 * {@link JGroupsRaftException} with the original class name and message. A type added to {@link #KNOWN} later is
 * received as the generic exception by nodes that do not know it yet.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class RemoteFailure {

    // A description string is written with writeUTF, up to this limit in bytes.
    // The full message is still available in the log for analysis.
    private static final int MAX_MESSAGE_LENGTH = 16_384;
    private static final String TRUNCATED = "... (truncated)";

    private static final Map<String, Function<String, Throwable>> KNOWN = Map.of(
            RaftLeaderException.class.getName(), RaftLeaderException::new,
            DegradedStateException.class.getName(), DegradedStateException::new
    );

    private RemoteFailure() { }

    /**
     * Describes a failure so it can be sent to the node waiting for the result of a request.
     *
     * <p>
     * Any failure can be described, including exceptions that cannot be serialized or that fail to provide a message.
     * The description identifies the failure for the caller and may hold only part of its message: the full message
     * and stack trace stay in this node's log.
     * </p>
     *
     * @param t The failure to describe.
     * @return The description, ready to send as the payload of a response.
     */
    public static byte[] encode(Throwable t) {
        try {
            ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(128);
            Bits.writeString(t.getClass().getName(), out);
            Bits.writeString(message(t), out);

            // Copying the byte buffer here because exceptions shouldn't be happening that frequently to thrash allocation.
            return Arrays.copyOf(out.buffer(), out.position());
        } catch (IOException e) {
            // Writing to in-memory buffer shouldn't fail.
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the part of a failure's message that is sent to the caller.
     *
     * <p>
     * The caller only needs enough to identify the failure, and an answer must always reach it, even when the message
     * is too large to send or cannot be read.
     * </p>
     *
     * @param t The failure being described.
     * @return The message to send, or {@code null} if the failure has none.
     */
    private static String message(Throwable t) {
        String message;
        try {
            // If for whatever reason a custom exception implementation fails with the get message.
            message = t.getMessage();
        } catch (Throwable ignore) {
            return null;
        }

        if (message == null || message.length() <= MAX_MESSAGE_LENGTH)
            return message;

        return message.substring(0, MAX_MESSAGE_LENGTH - TRUNCATED.length()) + TRUNCATED;
    }

    /**
     * Rebuilds a failure received from the node that processed a request.
     *
     * <p>
     * Failures that callers act on are rebuilt with their original type and message, so callers can handle them as if
     * they happened locally. Any other failure, including a type this node does not know yet, is returned as a
     * {@link JGroupsRaftException} with the original type name and message. A description that cannot be read is
     * still returned as a failure, since the caller is waiting for an answer.
     * </p>
     *
     * @param buf    The buffer holding the description.
     * @param offset The offset of the description in the buffer.
     * @param length The length of the description.
     * @return The failure to complete the caller's request with, never {@code null}.
     */
    public static Throwable decode(byte[] buf, int offset, int length) {
        try {
            ByteArrayDataInputStream in = new ByteArrayDataInputStream(buf, offset, length);
            String className = Bits.readString(in);
            String message = Bits.readString(in);

            Function<String, Throwable> factory = KNOWN.get(className);
            return factory != null
                    ? factory.apply(message)
                    : JGroupsRaftException.stackless(className + ": " + message);
        } catch (IOException e) {
            return new JGroupsRaftException("Failed reading the failure sent remotely", e);
        }
    }
}
