package org.jgroups.protocols.raft;

/**
 * Implements the behavior of a learner node.
 *
 * <p>
 * A learner nodes operates the same way as a {@link Follower}. However, the learner does not have voting rights for
 * committing an entry, for electing a leader, or to become a leader.
 * </p>
 *
 * @author José Bolina
 * @since 1.1
 * @see <a href="https://github.com/jgroups-extras/jgroups-raft/pull/356">Learner design</a>
 */
public final class Learner extends Follower {

    public Learner(RAFT raft) {
        super(raft);
    }
}
