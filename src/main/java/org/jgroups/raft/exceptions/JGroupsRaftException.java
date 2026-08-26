package org.jgroups.raft.exceptions;

/**
 * Base unchecked exception for JGroups Raft.
 *
 * @author José Bolina
 * @since 2.0
 */
public class JGroupsRaftException extends RuntimeException {

    public JGroupsRaftException() {
        super();
    }

    public JGroupsRaftException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

    public JGroupsRaftException(String msg) {
        super(msg);
    }

    public JGroupsRaftException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public JGroupsRaftException(String msg, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(msg, cause, enableSuppression, writableStackTrace);
    }

    public static JGroupsRaftException stackless(String message) {
        return new JGroupsRaftException(message, null, false, false);
    }
}
