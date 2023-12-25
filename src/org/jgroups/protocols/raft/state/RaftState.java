package org.jgroups.protocols.raft.state;

import java.util.Objects;
import java.util.function.Consumer;

import org.jgroups.Address;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.RAFT;

import net.jcip.annotations.ThreadSafe;

/**
 * Keep track of part of the algorithm state. The state here is tightly synchronized and needs to be updated atomically.
 * <p>
 * Throughout the RAFT execution, the node updates the values for term, leader, and the election vote. The values need
 * to be updated atomically, following the term transition. This synchronization is necessary for clients to perceive
 * the same leaders and terms.
 * <p>
 * Utilizing both Ongaro's dissertation<sup>1</sup> and the original RAFT<sup>2</sup> article as the base. The relation
 * of all these variables in the state may not be clear. Starting with the assertion that a term only starts through a
 * new election. In other words, when updating the term, the leader is unknown. The term increases monotonically, and
 * some nodes might skip it due to connectivity issues.
 * <p>
 * All messages carry the sender's current term. On receiving a message, nodes verify and update. Messages with older
 * terms are discarded. A higher term causes the node to advance its term, set both the leader and vote to null, and
 * proceed with the message processing.
 * <p>
 * The leader value only updates to the elected leader (null -> new leader) or to step down (leader -> null). Updating
 * the leader directly to another throws an exception. The election vote follows the same logic.
 * <p>
 * In summary, the logic follows that updating to a received higher term sets the leader and vote to null. Later, when
 * the leader is known, the update within the same term is accepted since the current term leader is still null.
 *
 * @author José Bolina
 * @since 1.0.12
 * @see <a href="https://github.com/ongardie/dissertation">1: Ongaro's dissertation</a>
 * @see <a href="https://www.usenix.org/node/184041.">2: RAFT paper</a>
 */
@ThreadSafe
public class RaftState {

    private final RAFT raft;
    private final Consumer<Address> onLeaderUpdate;
    private Address leader;
    private long term;
    private Address votedFor;

    public RaftState(RAFT raft, Consumer<Address> onLeaderUpdate) {
        this.raft = raft;
        this.onLeaderUpdate = onLeaderUpdate;
        reload();
    }

    public synchronized Address leader() {
        return leader;
    }

    public synchronized long currentTerm() {
        return term;
    }

    public synchronized Address votedFor() {
        return votedFor;
    }

    /**
     * Advances the term for leader election.
     * <p>
     * This automatically sets the {@link #leader} and {@link #votedFor} to null.
     * </p>
     *
     * @return The new term.
     * @see #tryAdvanceTerm(long)
     */
    public long advanceTermForElection() {
        while (true) {
            long current = currentTerm();
            if (tryAdvanceTerm(current + 1) == 1) {
                return currentTerm();
            }
        }
    }

    /**
     * Try advancing the term, and if succeeding, the leader is null.
     *
     * @param newTerm: The term to advance to.
     * @return Has the same result as <code>Long.compare(newTerm, currentTerm());</code>.
     * @see #tryAdvanceTermAndLeader(long, Address)
     */
    public int tryAdvanceTerm(long newTerm) {
        return tryAdvanceTermAndLeader(newTerm, null);
    }

    /**
     * Try to advance the current term and set the leader.
     * <p>
     * A lower term has no effect. A higher term updates the node's current term to {@param newTerm}, and the leader to
     * {@param newLeader} and vote to <code>null</code>. The term and vote are written to disk.
     *
     * @param newTerm: New term to update to.
     * @param newLeader: The leader to update in case the term advances.
     * @return Has the same result as <code>Long.compare(newTerm, currentTerm());</code>. That is, -1 if the
     *         <code>newTerm < currentTerm()</code>, 0 if both are equal, and 1 if <code>currentTerm()</code> is higher.
     */
    public int tryAdvanceTermAndLeader(long newTerm, Address newLeader) {
        boolean saveVote = false;
        boolean saveTerm = false;
        synchronized (this) {
            // Step down uses the term `0`. It does not update the term, only the leader to null.
            if (newTerm > 0 && newTerm < term) return -1;
            if (newTerm == term && newLeader == null) return 0;
            if (newTerm > term) {
                raft.getLog().trace("%s: changed term from %d -> %d", raft.addr(), term, newTerm);

                term = newTerm;
                saveTerm = true;
                saveVote = setVotedFor(null, false);
                // To not invoke the leader change listener twice.
                this.leader = null;
            }
            setLeader(newLeader);
        }

        Log log = raft.log();
        if (log != null) {
            if (saveTerm) log.currentTerm(currentTerm());
            if (saveVote) log.votedFor(votedFor());
        }

        return saveTerm ? 1 : 0;
    }

    /**
     * Update the leader in the current term.
     * <p>
     * A leader can not change within the same term. The update only happens when a new leader is elected
     * (null -> new leader) or when the leader steps down (current leader -> null).
     *
     * @param newLeader: The new leader to update to.
     * @throws IllegalStateException: If trying to update between different leaders in the same term.
     */
    public synchronized void setLeader(Address newLeader) {
        if (this.leader != null && newLeader != null && !Objects.equals(this.leader, newLeader))
            throw new IllegalStateException(String.format("Changing leader %s to %s illegally", this.leader, newLeader));

        boolean updated = newLeader == null || this.leader == null;
        this.leader = newLeader;

        // We only should invoke the listener when a new leader is set/step down.
        if (updated) {
            raft.getLog().trace("%s: change leader from %s -> %s", raft.addr(), leader, newLeader);
            onLeaderUpdate.accept(this.leader);
        }
    }

    /**
     * Set the vote in the current term.
     * <p>
     * Follows the same logic as {@link #setLeader(Address)}. The vote is also written to disk.
     *
     * @param votedFor: The vote in the current term.
     * @throws IllegalStateException: If trying to update between different votes within the same term.
     */
    public void setVotedFor(Address votedFor) {
        setVotedFor(votedFor, true);
    }

    /**
     * Read the state information from the {@link RAFT}'s log.
     */
    public void reload() {
        Log log = raft.log();
        if (log != null) {
            synchronized (this) {
                term = log.currentTerm();
                votedFor = log.votedFor();
            }
        }
    }

    private boolean setVotedFor(Address votedFor, boolean save) {
        synchronized (this) {
            if (votedFor != null && this.votedFor != null && !Objects.equals(votedFor, this.votedFor))
                throw new IllegalStateException(String.format("Changing vote %s to %s illegally", this.votedFor, votedFor));

            // If it is the same object, returns null as there is no need to flush to disk.
            if (Objects.equals(this.votedFor, votedFor)) return false;
            this.votedFor = votedFor;
        }

        if (save) {
            Log log = raft.log();
            if (log != null) log.votedFor(votedFor());
        }
        return true;
    }

    @Override
    public String toString() {
        return "[leader=" + leader + ", term=" + term + ", voted=" + votedFor + "]";
    }
}
