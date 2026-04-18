package org.jgroups.protocols.raft;

/**
 * Callback notified when the underlying storage layer encounters an unrecoverable failure.
 *
 * <p>
 * The listener is invoked at most once per adapter lifetime. After notification, the adapter rejects all subsequent
 * storage-mutating operations.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@FunctionalInterface
interface LogFailureListener {

    /**
     * Called when the log detects an unrecoverable storage failure.
     *
     * @param cause the underlying exception that triggered the failure
     */
    void onLogFailure(Throwable cause);
}
