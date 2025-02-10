package org.jgroups.raft.exceptions;

/**
 * Base unchecked exception for JGroups Raft.
 *
 * @author José Bolina
 * @since 2.0
 */
public class JRaftException extends RuntimeException {

    public JRaftException() {
        super();
    }

    public JRaftException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

    public JRaftException(String msg) {
        super(msg);
    }

    public JRaftException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public JRaftException(String msg, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(msg, cause, enableSuppression, writableStackTrace);
    }
}
