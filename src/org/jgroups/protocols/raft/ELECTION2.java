package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.protocols.raft.election.PreVoteRequest;
import org.jgroups.protocols.raft.election.PreVoteResponse;
import org.jgroups.raft.util.Utils;
import org.jgroups.raft.util.Utils.Majority;
import org.jgroups.util.ResponseCollector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.jgroups.Message.Flag.OOB;

/**
 * A leader election protocol.
 * <p>
 * This implementation extends {@link ELECTION} with a pre-vote mechanism. The pre-vote always runs before starting the
 * voting thread to define the leader. The pre-vote increases the delay in electing the leader but, in turn, covers more
 * edge cases. As a rule of thumb, if deploying in an unstable network with frequent partitions, this protocol should
 * give a more stable mechanism, avoid leader disruptions, and avoid possible liveness issues. Otherwise,
 * {@link ELECTION} is the default choice.
 *
 * <p><h3>Pre-Voting phase:</h3>
 *
 * This extension includes the pre-voting mechanism proposed in Ongaro's dissertation (§9.6). In the current
 * implementation, a pre-voting phase starts in case the current node is the new view coordinator and:
 *
 * <ul>
 *     <li>The view coordinator changes and the computed update is {@link Majority#no_change};</li>
 *     <li>The computed view update is {@link Majority#reached} or {@link Majority#leader_lost}</li>
 * </ul>
 *
 * The process which executes the pre-voting mechanism sends a {@link PreVoteRequest} to all processes in the view. The
 * recipients reply with a {@link PreVoteResponse} identifying the node they see as leader. Once the initiator receives
 * the reply from all nodes in the view, it can start the voting process, resuming the work the same as {@link ELECTION}.
 *
 * @since 1.0.12
 * @see ELECTION
 * @see <a href="https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf">Ongaro's dissertation</a>
 * @see <a href="https://github.com/belaban/jgroups-raft/issues/211">Issue #221</a>
 * @author José Bolina
 */
@MBean(description = "Performs leader election with a pre-voting phase.")
public class ELECTION2 extends BaseElection {

    protected static final short ELECTION_ID    = 524;

    static {
        ClassConfigurator.addProtocol(ELECTION_ID, ELECTION2.class);
        ClassConfigurator.add(PRE_VOTE_REQ, PreVoteRequest.class);
        ClassConfigurator.add(PRE_VOTE_RSP, PreVoteResponse.class);
    }

    private final PreVotingMechanism preVotingMechanism = new PreVotingMechanism();

    @ManagedAttribute(description="Whether the pre-voting is running? (Coordinator only)")
    public boolean isPreVoteThreadRunning() {
        return preVotingMechanism.isRunning();
    }

    @Override
    protected void handleView(View v) {
        Majority result = Utils.computeMajority(view, v, raft().majority(), raft.leader());
        log.debug("%s: existing view: %s, new view: %s, result: %s", local_addr, this.view, v, result);

        View old_view = this.view;
        this.view = v;

        List<Address> joiners = View.newMembers(old_view, v);
        boolean has_new_members = joiners != null && !joiners.isEmpty();

        switch (result) {
            case no_change:
                if (raft.isLeader() && has_new_members) {
                    sendLeaderElectedMessage(raft.leader(), raft.currentTerm());
                    break;
                }
                // If we have no change in terms of majority threshold. If the view coordinator changed, we need to
                // verify if an election is necessary.
                if (viewCoordinatorChanged(old_view, v) && isViewCoordinator() && view.size() >= raft.majority()) {
                    preVotingMechanism.start();
                }
                break;
            case reached:
            case leader_lost:
                if (isViewCoordinator()) {
                    preVotingMechanism.start();
                }
                break;
            case lost:
                preVotingMechanism.stop();
                stopVotingThread();
                raft.setLeaderAndTerm(null);
                break;
        }
    }

    @Override
    protected void handleMessage(Message msg, RaftHeader hdr) {
        if (hdr instanceof PreVoteRequest) {
            handlePreVoteRequest(msg, (PreVoteRequest) hdr);
            return;
        }

        if (hdr instanceof PreVoteResponse) {
            handlePreVoteResponse(msg, (PreVoteResponse) hdr);
            return;
        }

        super.handleMessage(msg, hdr);
    }

    private static boolean viewCoordinatorChanged(View old_view, View curr) {
        if (old_view == null) return true;
        return !Objects.equals(old_view.getCoord(), curr.getCoord());
    }

    /**
     * Handle the {@link PreVoteRequest} coming from other nodes.
     * <p>
     * A node sends a {@link PreVoteRequest} to verify if it can become the leader, running the pre-voting phase
     * instead of disrupting the cluster. The node that receives this request must only reply if they would vote for
     * the sender in an election. Although, they can reply to different pre-votes, the node is not bound during this phase.
     * <p>
     * This version is an altered version of the pre-voting mechanism from the dissertation (§9.6). In this version,
     * the node replies with its current known leader address. This is because the sender is not interested in electing
     * itself. The sender is checking if a cluster-wide election round should be started.
     *
     * @param message: The message received.
     * @param hdr: The message header.
     */
    private void handlePreVoteRequest(Message message, PreVoteRequest hdr) {
        sendPreVoteResponse(message.getSrc());
    }

    private void handlePreVoteResponse(Message msg, PreVoteResponse hdr) {
        // We are not interested in the replies if the voting phase started.
        // Or if we are not the view coordinator.
        if (isVotingThreadRunning() || !isViewCoordinator() || !preVotingMechanism.isRunning()) return;

        preVotingMechanism.includeResponse(msg.getSrc(), hdr);
    }

    private void sendPreVotingRequest() {
        PreVoteRequest hdr = new PreVoteRequest();
        Message msg = new EmptyMessage(null).putHeader(id, hdr).setFlag(OOB);
        log.trace("%s -> all: %s", local_addr, hdr);
        down_prot.down(msg);
    }

    private void sendPreVoteResponse(Address dest) {
        PreVoteResponse hdr = new PreVoteResponse(raft.leader());
        Message msg = new EmptyMessage(dest).putHeader(id, hdr).setFlag(OOB);
        log.trace("%s -> %s: %s", local_addr, dest, hdr);
        down_prot.down(msg);
    }

    private class PreVotingMechanism {
        protected final ResponseCollector<PreVoteResponse> preVotingResponses = new ResponseCollector<>();

        public boolean isRunning() {
            return preVotingResponses.size() > 0;
        }

        public void start() {
            int majority = raft.majority();
            if (!isRunning() && isViewCoordinator() && !isVotingThreadRunning() && view.getMembers().size() >= majority) {
                log.trace("%s: starting pre-voting mechanism", local_addr);
                startPreVotingPhase();
            }
        }

        public void stop() {
            log.trace("%s: stopping pre-voting thread", local_addr);
            preVotingResponses.reset();
        }

        /**
         * Include a new response to the running pre-vote phase.
         * <p>
         * Once all responses are collected and there is still a majority, the responses are parsed to verify if an
         * election phase should start.
         *
         * @param sender: The response sender.
         * @param hdr: The response message.
         */
        public void includeResponse(Address sender, PreVoteResponse hdr) {
            preVotingResponses.add(sender, hdr);;

            int majority = raft.majority();
            if (preVotingResponses.hasAllResponses() && preVotingResponses.numberOfValidResponses() >= majority) {
                Map<Address, PreVoteResponse> responses = Map.copyOf(preVotingResponses.getResults());
                stopPreVotingPhase(responses);
                stop();
            } else if (log.isTraceEnabled()) {
                log.trace("%s: collected pre-vote responses %s", local_addr, preVotingResponses.getResults());
            }
        }

        private void startPreVotingPhase() {
            preVotingResponses.reset(view.getMembers());
            sendPreVotingRequest();
        }

        /**
         * Executes after collecting messages from <b>all</b> nodes in the current view.
         * <p>
         * This procedure parses the responses to identify whether to start the voting thread. It has the strategy of:
         *
         * <ul>
         *     <li>A majority of nodes does not have a leader or see the same "outdated" leader as this node;
         *     <li>In case a majority sees a different leader:
         *     <ul>
         *         <li>The supposed leader is not in the view;
         *         <li>The supposed leader does not see itself as leader;
         *     </ul>
         * </ul>
         * <p>
         * In case none of that matches, the election algorithm is not started.
         *
         * @param responses The cluster {@link PreVoteResponse} responses. Must have a response from all nodes
         *                  in {@link #view}.
         */
        private void stopPreVotingPhase(Map<Address, PreVoteResponse> responses) {
            int acceptStartVoting = 0;
            Address localLeader = raft.leader();

            log.trace("%s: validating pre-vote responses from %s", local_addr, responses);

            Address remoteLeader = null;
            for (PreVoteResponse response : responses.values()) {
                if (response == null) continue;

                Address leader = response.leader();

                // The remote node either does not have a leader (== null) or has the same leader as this node.
                // If we are at this stage, means that this node suspects the leader. For example, we have merged partitions
                // and the remote still sees the old leader, or the node receive the message before the view update event.
                // We count that towards starting the voting thread.
                if (leader == null || leader.equals(localLeader)) {
                    acceptStartVoting++;
                } else {
                    // Is it possible that a node is so far behind came to life with a very old leader?
                    assert remoteLeader == null || remoteLeader.equals(leader) : "Somehow the leader is different!!";
                    remoteLeader = leader;
                }
            }

            // We have a majority already. Most of the nodes see an outdated or does not have a leader.
            if (acceptStartVoting >= raft.majority()) {
                log.debug("%s: pre-voting phase finished and starting the voting phase", local_addr);

                // We can already remove the outdated leader.
                raft.setLeaderAndTerm(null);
                startVotingThread();
                return;
            }

            log.trace("%s: did not met majority, taking slow-path %s", local_addr, responses);

            // We did not meet the majority. We need to inspect the responses more closely. In a more concrete example:
            // Given a cluster V1 = {A, B, C, D, E} with leader A. Suffering a quorum loss with a partial connectivity,
            // having views: A and C have V2 = {A, C} and B, D, E still have V1.
            // Once the connectivity is restored, A and C will merge the views and have V3 = {A, B, C, D, E}. Given that
            // node A is the coordinator and executes the pre-voting, a majority still sees A as leader. This leads to a
            // liveness scenario where a majority still sees the old leader, but A stepped down, and a new voting
            // phase is never started.
            Map<Address, Integer> votes = new HashMap<>();
            for (PreVoteResponse response : responses.values()) {
                if (response == null || response.leader() == null) continue;
                votes.compute(response.leader(), (k, v) -> v == null ? 1 : v + 1);
            }

            // Compute the leader the cluster still see.
            Address leader = null;
            for (Map.Entry<Address, Integer> entry : votes.entrySet()) {
                if (leader == null || entry.getValue() > votes.get(leader)) {
                    leader = entry.getKey();
                }
            }

            assert leader != null: "Leader should not be null at this stage: " + responses;
            PreVoteResponse response = responses.get(leader);

            // If the response is null, means the old leader is not present in the current view.
            // The other possibility was detailed above, a cluster see the leader, but the node stepped down, and
            // it does not see itself as a leader.
            if (response == null || !leader.equals(response.leader())) {
                log.debug("%s: pre-voting phase finished and starting the voting phase", local_addr);
                startVotingThread();
                return;
            }

            // Should receive a late message from the leader?
            log.trace("%s: not able to start voting, majority sees %s as leader", local_addr, leader);
        }
    }
}
