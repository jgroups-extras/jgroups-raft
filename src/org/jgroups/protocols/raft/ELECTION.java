package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.util.Utils;
import org.jgroups.raft.util.Utils.Majority;

import java.util.List;

/**
 * The default leader election algorithm.
 * <p>
 * Performs leader election. This implementation takes full advantage of JGroup's membership events with {@link View}.
 * When the current node is the view coordinator, it starts a voting thread to ask all members to send their information.
 * The voting thread stops when a new leader is elected.
 * <p>
 * The process that starts the voting thread is not trying to elect itself. The process running the voting process
 * increases its term and asks all nodes about their term and log index information to select the new leader, in the
 * form of {@link org.jgroups.protocols.raft.election.VoteResponse}. For safety reasons, only the nodes with the most
 * up-to-date log can be elected a leader. With a response from the majority processes, the leader with the higher term
 * and log index is elected. The oldest process (view coordinator) in the system has a priority. Once decided, the
 * process sends a message reliably to everyone identifying the new leader, with the
 * {@link org.jgroups.protocols.raft.election.LeaderElected} message.
 * <p>
 * After a leader is elected, a new election round starts on view changes only if the leader left the cluster. In case
 * of losing a majority, the leader steps down.
 * <p>
 * This implementation is more robust than building with heartbeats, leading to fewer disruptions in the cluster with
 * unnecessary (competing) election rounds. This also means the leader is capable of stepping down. Referred to in
 * §6.2 of Ongaro's dissertation to prevent stale leadership information.
 * <p>
 * More information is available in the design docs.
 *
 * @author Bela Ban
 * @since  0.1
 * @see <a href="https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf">Ongaro's dissertation</a>
 */
@MBean(description="Protocol performing leader election according to the RAFT paper")
public class ELECTION extends BaseElection {
    protected static final short ELECTION_ID    = 520;

    static {
        ClassConfigurator.addProtocol(ELECTION_ID, ELECTION.class);
    }

    @Override
    protected void handleView(View v) {
        Majority result=Utils.computeMajority(view, v, raft().majority(), raft.leader());
        log.debug("%s: existing view: %s, new view: %s, result: %s", local_addr, this.view, v, result);
        List<Address> joiners=View.newMembers(this.view, v);
        boolean has_new_members=joiners != null && !joiners.isEmpty();
        boolean coordinatorChanged = Utils.viewCoordinatorChanged(this.view, v);
        this.view=v;
        switch(result) {
            case no_change:
                // the leader resends its term/address for new members to set the term/leader.
                if(raft.isLeader() && has_new_members)
                    sendLeaderElectedMessage(raft.leader(), raft.currentTerm());

                // Handle cases where the previous coordinator left *before* a leader was elected.
                // See: https://github.com/jgroups-extras/jgroups-raft/issues/259
                else if (coordinatorChanged && isViewCoordinator() && isMajorityAvailable() && raft.leader() == null)
                    startVotingThread();
                break;
            case reached:
            case leader_lost:
                // In case the leader is lost, we stop everything *before* starting again.
                // This avoids cases where the leader is lost before the voting mechanism has stopped.
                // See: https://github.com/jgroups-extras/jgroups-raft/issues/259
                if(isViewCoordinator()) {
                    log.trace("%s: starting voting process (reason: %s, view: %s)", local_addr, result, view);
                    stopVotingThread();
                    startVotingThread();
                }
                break;
            case lost:
                stopVotingThread(); // if running, double-dutch
                raft.setLeaderAndTerm(null);
                break;
        }
    }
}
