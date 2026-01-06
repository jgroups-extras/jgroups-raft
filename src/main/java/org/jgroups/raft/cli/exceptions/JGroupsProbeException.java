package org.jgroups.raft.cli.exceptions;

/**
 * A specialized runtime exception for errors originating from the JGroups Probe protocol.
 *
 * <p>
 * This exception is typically thrown when a probe request fails due to network issues, timeout, or an inability to
 * communicate with the cluster nodes. It serves as a wrapper to differentiate protocol-specific failures from general
 * application errors.
 * </p>
 * <p>
 * The {@link JGroupsRaftExceptionHandler} detects this exception to produce specific exit codes
 * (see {@link JGroupsRaftExceptionHandler#PROBE_EXCEPTION_CODE}).
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see JGroupsRaftExceptionHandler
 */
public class JGroupsProbeException extends RuntimeException {
    private final int exitCode;

    /**
     * Constructs a new exception with the specified detail message, exit code, and cause.
     *
     * @param message  The detail message explaining the reason for the failure.
     * @param exitCode The specific process exit code to be returned by the CLI.
     * @param cause    The underlying cause (e.g., a SocketTimeoutException or IOException).
     */
    public JGroupsProbeException(String message, int exitCode, Throwable cause) {
        super(message, cause);
        this.exitCode = exitCode;
    }

    /**
     * Returns the exit code associated with this exception.
     *
     * @return The integer exit code.
     */
    public int exitCode() {
        return exitCode;
    }
}
