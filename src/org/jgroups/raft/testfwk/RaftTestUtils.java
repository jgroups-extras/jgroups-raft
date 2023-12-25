package org.jgroups.raft.testfwk;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Utilities for developing tests with Raft.
 *
 * @since 1.0.13
 * @author José Bolina
 */
public final class RaftTestUtils {

    private RaftTestUtils() { }

    /**
     * Retrieves the {@link RAFT} protocol from the provided {@link JChannel}.
     *
     * @param ch: The channel to search the protocol.
     * @return The {@link RAFT} instance or <code>null</code> if not found.
     */
    public static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }

    /**
     * Retrieves the {@link BaseElection} protocol from the provided {@link JChannel}.
     * The concrete type might vary according to the configured protocol stack.
     *
     * @param ch: The channel to search the protocol.
     * @return The {@link BaseElection} instance or <code>null</code> if not found.
     */
    public static BaseElection election(JChannel ch) {
        return ch.getProtocolStack().findProtocol(BaseElection.class);
    }

    /**
     * Checks if the node is the current {@link RAFT} leader.
     * <p>
     * This method search the protocol stack to retrieve the {@link RAFT} instance and then verifies if elected.
     * </p>
     *
     * @param ch: The channel to verify.
     * @return <code>true</code> if currently the leader, and <code>false</code> otherwise
     *         or not found the {@link RAFT} protocol.
     */
    public static boolean isRaftLeader(JChannel ch) {
        RAFT r = raft(ch);
        return r.isLeader() && r.leader() != null && ch.getAddress().equals(r.leader());
    }

    /**
     * Checks that given the time constraints, eventually {@link #isRaftLeader(JChannel)} is <code>true</code>.
     *
     * @param ch: Channel to verify.
     * @param timeoutMs: The timeout in milliseconds.
     * @return <code>true</code> if it became the leader, <code>false</code>, otherwise.
     * @see #isRaftLeader(JChannel)
     */
    public static boolean eventuallyIsRaftLeader(JChannel ch, long timeoutMs) {
        return eventually(() -> isRaftLeader(ch), timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Deletes all the replicated data and internal state for the given {@link RAFT} instance.
     * <p>
     * This is useful when multiple tests for the same uses stable storage. After running each test the data can
     * be deleted to no affect subsequent tests.
     * </p>
     *
     * @param r: {@link RAFT} instance to delete all information.
     * @throws Exception: If an exception happens while deleting the data.
     * @see Log#delete()
     */
    public static void deleteRaftLog(RAFT r) throws Exception {
        Log log = r != null ? r.log() : null;
        if (log != null) {
            log.delete();
            r.log(null);
        }
    }

    /**
     * Verify that in the given time constraints, the expression returns <code>true</code>.
     *
     * @param bs: Boolean expression to verify.
     * @param timeout: Timeout value.
     * @param unit: Timeout unit.
     * @return <code>true</code> if expression valid before time out, <code>false</code>, otherwise.
     * @throws RuntimeException: If an exception is thrown while verifying the expression.
     */
    public static boolean eventually(BooleanSupplier bs, long timeout, TimeUnit unit) {
        try {
            long timeoutNanos = unit.toNanos(timeout);
            // We want the sleep time to increase in arithmetic progression
            // 30 loops with the default timeout of 30 seconds means the initial wait is ~ 65 millis
            int loops = 30;
            int progressionSum = loops * (loops + 1) / 2;
            long initialSleepNanos = timeoutNanos / progressionSum;
            long sleepNanos = initialSleepNanos;
            long expectedEndTime = System.nanoTime() + timeoutNanos;
            while (expectedEndTime - System.nanoTime() > 0) {
                if (bs.getAsBoolean())
                    return true;
                LockSupport.parkNanos(sleepNanos);
                sleepNanos += initialSleepNanos;
            }

            return bs.getAsBoolean();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected!", e);
        }
    }
}
