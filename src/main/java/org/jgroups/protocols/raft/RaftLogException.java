package org.jgroups.protocols.raft;

/**
 * Unchecked exception thrown by {@link RaftLogAdapter} after the log has been poisoned due to an unrecoverable storage
 * failure.
 *
 * <p>
 * Once thrown, the exception instance is reused for all subsequent operations on the same adapter. Callers can compare
 * by reference to detect repeated failures.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see RaftLogAdapter
 */
public class RaftLogException extends RuntimeException {
    public RaftLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
