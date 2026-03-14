package org.jgroups.protocols.raft.election;

import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.raft.internal.metrics.ElectionProtocolMetrics;
import org.jgroups.raft.metrics.LatencyMetrics;
import org.jgroups.raft.util.RaftClassConfigurator;
import org.jgroups.raft.util.TimeService;
import org.jgroups.raft.util.Utils;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.ResponseCollector;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Abstract leader election algorithm functionalities.
 * <p>
 * This abstraction includes the voting mechanism and handles messages and events from the stack. All the election
 * algorithms extend this class. For more information, see the concrete implementations.
 *
 * @author Bela Ban
 * @see org.jgroups.protocols.raft.ELECTION
 * @see org.jgroups.protocols.raft.ELECTION2
 */
public abstract class BaseElection extends Protocol {

    public static final short VOTE_REQ = 3000;
    public static final short VOTE_RSP = 3001;
    public static final short LEADER_ELECTED = 3005;
    public static final short PRE_VOTE_REQ = 3006;
    public static final short PRE_VOTE_RSP = 3007;

    static {
        RaftClassConfigurator.initialize();
    }

    protected RAFT raft;

    private final ElectionRunner voting_thread = new ElectionRunner("voting-thread", this::runVotingProcess);
    private final ResponseCollector<VoteResponse> votes = new ResponseCollector<>();
    private volatile boolean stopVoting;

    private ElectionProtocolMetrics metrics;
    private volatile long electionStartNanos;
    private volatile Instant electionStart;
    private volatile Instant electionEnd;

    @Property(description = "Max time (ms) to wait for vote responses", type = AttributeType.TIME)
    protected long vote_timeout = 600;

    @ManagedAttribute(description = "Number of voting rounds initiated by the coordinator")
    protected int num_voting_rounds;

    protected volatile View view;

    public long voteTimeout() {
        return vote_timeout;
    }

    /**
     * Defines the default timeout in milliseconds to utilize during any election operation.
     *
     * @param timeoutMs Timeout value in milliseconds.
     * @return This election instance.
     * @throws IllegalArgumentException In case timeout is less than or equal to 0.
     */
    public BaseElection voteTimeout(long timeoutMs) {
        if (timeoutMs <= 0) throw new IllegalArgumentException("Timeout should be greater than 0.");

        this.vote_timeout = timeoutMs;
        return this;
    }

    public RAFT raft() {
        return raft;
    }

    public BaseElection raft(RAFT r) {
        raft = r;
        return this;
    }

    public ResponseCollector<VoteResponse> getVotes() {
        return votes;
    } // use for testing only!

    @ManagedAttribute(description = "Is the voting thread (only on the coordinator) running?")
    public synchronized boolean isVotingThreadRunning() {
        return voting_thread.isRunning();
    }

    @ManagedOperation(description = "Trigger the voting process (only on the coordinator)")
    public boolean runVotingThread() {
        if (isViewCoordinator()) {
            startVotingThread();
            return isVotingThreadRunning();
        }
        return false;
    }

    @ManagedAttribute(description = "Instant a new election started", type = AttributeType.TIME)
    public Instant electionStart() {
        return electionStart;
    }

    @ManagedAttribute(description = "Instant a new election ended", type = AttributeType.TIME)
    public Instant electionEnd() {
        return electionEnd;
    }

    @ManagedAttribute(description = "Time in milliseconds since the last election ended", type = AttributeType.TIME)
    public long timeSinceLastElection() {
        if (electionEnd == null)
            return -1;

        return Duration.between(electionEnd, raft.timeService().now()).toMillis();
    }

    @ManagedAttribute(description = "Mean latency for leader election in nanoseconds", type = AttributeType.TIME, unit = TimeUnit.NANOSECONDS)
    public double electionMeanLatency() {
        if (metrics == null)
            return -1;

        return metrics.election().getAvgLatency();
    }

    protected abstract void handleView(View v);

    @Override
    public void resetStats() {
        super.resetStats();
        num_voting_rounds = 0;
        metrics = (raft != null && raft.statsEnabled()) || statsEnabled() ? new ElectionProtocolMetrics() : null;
    }

    @Override
    public void init() throws Exception {
        super.init();
        raft = RAFT.findProtocol(RAFT.class, this, false);
    }

    @Override
    public void start() throws Exception {
        super.start();
        if ((raft != null && raft.statsEnabled()) || statsEnabled())
            metrics = new ElectionProtocolMetrics();
    }

    @Override
    public void stop() {
        stopVotingThread();
        if (raft != null)
            raft.setLeaderAndTerm(null);
    }

    @Override
    public Object down(Event evt) {
        switch (evt.getType()) {
            case Event.DISCONNECT:
                raft.setLeaderAndTerm(null);
                break;
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
            default:
                break;
        }
        return down_prot.down(evt);
    }

    @Override
    public Object up(Event evt) {
        if (evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return up_prot.up(evt);
    }

    @Override
    public Object up(Message msg) {
        RaftHeader hdr = msg.getHeader(id);
        if (hdr != null) {
            handleMessage(msg, hdr);
            return null;
        }
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        for (Iterator<Message> it = batch.iterator(); it.hasNext(); ) {
            Message msg = it.next();
            RaftHeader hdr = msg.getHeader(id);
            if (hdr != null) {
                it.remove();
                handleMessage(msg, hdr);
            }
        }
        if (!batch.isEmpty())
            up_prot.up(batch);
    }

    /**
     * @return election latency metrics, or disabled metrics if stats are off.
     */
    public LatencyMetrics electionLatency() {
        return metrics != null ? metrics.election() : LatencyMetrics.disabled();
    }

    protected void handleMessage(Message msg, RaftHeader hdr) {
        if (hdr instanceof LeaderElected) {
            handleLeaderElected(msg, (LeaderElected) hdr);
            return;
        }

        if (hdr instanceof VoteRequest) {
            handleVoteRequest(msg, (VoteRequest) hdr);
            return;
        }

        if (hdr instanceof VoteResponse) {
            handleVoteResponse(msg, (VoteResponse) hdr);
        }
    }

    protected boolean isViewCoordinator() {
        return Objects.equals(this.view.getCoord(), local_addr);
    }

    private void handleLeaderElected(Message msg, LeaderElected hdr) {
        long term = hdr.currTerm();
        Address leader = hdr.leader();
        View v = this.view;
        // Only receive messages with null view when running tests with mock cluster.
        // Otherwise, need to make sure the leader is the current view and there's still a majority.
        // The view could change between the leader is decided and the message arrives.
        if (v == null || (isLeaderInView(leader, v) && isMajorityAvailable(v, raft))) {
            // Tries to install the new leader.
            // We only ever stop the voting thread in case the installation goes through.
            // This is necessary to handle partitioning cases with coordinator changes in the view.
            // See: https://github.com/jgroups-extras/jgroups-raft/issues/306
            int res = raft.trySetLeaderAndTerm(leader, term); // possibly changes the role
            log.trace("%s <- %s: %s (%d)", local_addr, msg.src(), hdr, res);
            if (res >= 0) {
                stopVotingThread(); // only on the coord

                // Non-coordinator members do not initiate the voting process, so we only track the instant it ended.
                if (!isViewCoordinator()) {
                    electionEnd = raft.timeService().now();
                }
            }
        } else {
            log.trace("%s <- %s: %s after leader left (%s)", local_addr, msg.src(), hdr, v);
        }
    }

    /**
     * Handle a received VoteRequest sent by the coordinator; send back the last log term and index. Restricts execution
     * to a single process per term. Do not reply the request if voted for another process in the same term.
     *
     * @param msg The received message.
     * @param hdr The received header.
     */
    private void handleVoteRequest(Message msg, VoteRequest hdr) {
        Address sender = msg.src();
        long new_term = hdr.currTerm();
        if (log.isTraceEnabled())
            log.trace("%s <- %s: VoteRequest(term=%d)", local_addr, sender, new_term);

        int result = raft.currentTerm(new_term);
        switch (result) {
            case -1: // new_term < current_term
                log.trace("%s: received vote request from %s with term=%d (current_term=%d); dropping vote response",
                        local_addr, sender, new_term, raft.currentTerm());
                return;
            case 1: // new_term > current_term
                log.trace("%s: received vote request from %s with higher term %d; accepting request",
                        local_addr, sender, new_term);
                break;
            case 0:
                log.trace("%s: received vote request from %s at same term %d", local_addr, sender, new_term);
                break;
            default:
                throw new IllegalStateException("Unknown term result: " + result);
        }

        Address voted_for = raft.votedFor();
        if (voted_for != null && !voted_for.equals(sender)) {
            log.trace("%s: already voted (for %s) in term %d; dropping vote request from %s",
                    local_addr, voted_for, new_term, sender);
            return;
        }

        if (!raft.members().contains(raft.raftId())) {
            log.trace("%s: is a learner, it does not vote; dropping request from %s",
                    local_addr, sender);
            return;
        }

        Log log_impl = raft.log();
        if (log_impl == null)
            return;
        raft.votedFor(sender);
        long my_last_index = log_impl.lastAppended();
        LogEntry entry = log_impl.get(my_last_index);
        long my_last_term = entry != null ? entry.term() : 0;
        sendVoteResponse(sender, new_term, my_last_term, my_last_index);
    }

    private void handleVoteResponse(Message msg, VoteResponse hdr) {
        votes.add(msg.src(), hdr);
    }

    protected Address determineLeader(Address exclude) {
        Address leader = null;
        VoteResponse highest = null;
        Map<Address, VoteResponse> results = votes.getResults();
        for (Address mbr : view.getMembersRaw()) {
            VoteResponse rsp = results.get(mbr);
            if (rsp == null)
                continue;
            if (highest == null || rsp.compareTo(highest) > 0) {
                leader = mbr;
                highest = rsp;
            }
        }

        // Excluded node is the highest, find a runner-up with the same term/index.
        // Equal term and index means equally up-to-date, safe to elect.
        // No runner-up means the excluded node is strictly ahead, return null to signal retry.
        if (Objects.equals(leader, exclude) && highest != null) {
            for (Address mbr : view.getMembersRaw()) {
                if (Objects.equals(mbr, exclude))
                    continue;
                VoteResponse rsp = results.get(mbr);
                if (rsp != null && rsp.compareTo(highest) == 0)
                    return mbr;
            }
            return null;
        }

        return leader;
    }

    /**
     * The mechanism for deciding a new leader.
     *
     * <p>
     * Increases its term and queries all participants about their term and log index information. The thread blocks
     * (with a timeout) waiting for responses. If the majority replies, the process with the highest term and index is elected.
     * The process keeps running until a leader is elected or the majority is lost.
     * </p>
     *
     * @param exclude address to exclude from leader candidacy, or {@code null} for no exclusion.
     * @param result  future to complete with the elected leader's address, or exceptionally on failure.
     *                If cancelled by the caller, the voting thread stops.
     */
    private void runVotingProcess(Address exclude, CompletableFuture<Address> result) {
        // If the thread is interrupted, means the voting thread was already stopped.
        // We place this here just as a shortcut to not increase the term in RAFT.
        if (Thread.interrupted()) {
            stopVotingThreadInternal();
            result.completeExceptionally(new InterruptedException("Election voting thread interrupted"));
            return;
        }

        // The caller cancelled the future, stop the voting thread.
        if (result.isCancelled() || result.isCompletedExceptionally()) {
            stopVotingThreadInternal();
            return;
        }

        // If externally put to stop, verify if is possible to stop.
        if (stopVoting && (!isMajorityAvailable() || raft.leader() != null)) {
            // Only stop in case there is no majority or the leader is already null.
            // Otherwise, keep running.
            stopVotingThreadInternal();

            // A leader may have been set externally, just to be on the safe side.
            Address leader = raft.leader();
            if (leader != null) {
                result.complete(leader);
            } else {
                result.completeExceptionally(new IllegalStateException("Election stopped without electing a leader"));
            }
            return;
        }

        View electionView = this.view;
        long new_term = raft.createNewTerm();
        votes.reset(filterToRaftMembers(electionView.getMembersRaw()));
        num_voting_rounds++;

        TimeService time = raft.timeService();
        long start = time.nanos();
        sendVoteRequest(new_term);

        // wait for responses from all members or for vote_timeout ms, whichever happens first:
        votes.waitForAllResponses(vote_timeout);
        long duration = TimeUnit.NANOSECONDS.toMillis(time.interval(start));

        int majority = raft.majority();
        if (votes.numberOfValidResponses() >= majority) {
            Address leader = determineLeader(exclude);

            // The excluded node has the highest term/index.
            // Do not elect a less up-to-date node, instead we retry the round.
            // In a healthy cluster, followers will have caught up and a non-excluded node will tie for highest on a future round.
            if (leader == null && exclude != null) {
                log.trace("%s: excluded node %s is strictly ahead, retrying", local_addr, exclude);
                stopVoting = false;
                return;
            }

            log.trace("%s: collected votes from %s in %d ms (majority=%d) -> leader is %s (new_term=%d)",
                    local_addr, votes.getResults(), duration, majority, leader, new_term);

            // Set as leader locally before sending the message.
            // This should avoid any concurrent joiners. See: https://github.com/jgroups-extras/jgroups-raft/issues/253
            raft.setLeaderAndTerm(leader, new_term);
            sendLeaderElectedMessage(leader, new_term); // send to all - self

            // Hold intrinsic lock while verifying.
            // If a view updates while this verification happens, it could lead to liveness issue where the
            // voting thread does not continue to run, keeping a leader that left the cluster.
            synchronized (this) {
                // Check whether majority still in place between the collection of all votes to determining the leader.
                // We must stop the voting thread and set the leader as null.
                if (!isMajorityAvailable()) {
                    log.trace("%s: majority lost (%s) before elected (%s)", local_addr, view, leader);
                    stopVotingThreadInternal();
                    raft.setLeaderAndTerm(null);
                    result.completeExceptionally(new IllegalStateException("Majority lost during election"));
                    return;
                }

                // At this point, the majority still in place, so we confirm the elected leader is still present in the view.
                // If the leader is not in the view anymore, we keep the voting thread running.
                if (isLeaderInView(leader, view)) {
                    stopVotingThreadInternal();
                    result.complete(leader);
                    return;
                }
            }

            if (log.isTraceEnabled())
                log.trace("%s: leader (%s) not in view anymore, retrying", local_addr, leader);

            stopVoting = false;
        } else if (log.isTraceEnabled())
            log.trace("%s: collected votes from %s in %d ms (majority=%d); starting another voting round",
                    local_addr, votes.getValidResults(), duration, majority);
    }

    private Collection<Address> filterToRaftMembers(Address... allMembers) {
        Collection<String> raftMembers = raft.members();
        return Arrays.stream(allMembers)
                .filter(address -> raftMembers.contains(Utils.extractRaftId(address)))
                .collect(Collectors.toList());
    }

    private static boolean isLeaderInView(Address leader, View view) {
        return view.containsMember(leader);
    }

    protected final boolean isMajorityAvailable() {
        return isMajorityAvailable(view, raft);
    }

    private static boolean isMajorityAvailable(View view, RAFT raft) {
        return view != null && view.size() >= raft.majority();
    }

    /**
     * Starts a forced leader election, excluding the given address from candidacy.
     *
     * <p>
     * Must be called on the view coordinator. The excluded address still participates in quorum
     * (votes are collected from it), but it is skipped when determining the leader.
     * </p>
     *
     * @param exclude the address to exclude from leader candidacy, or {@code null} for no exclusion.
     * @return a stage that completes with the newly elected leader's address.
     */
    public synchronized CompletionStage<Address> startForcedElection(Address exclude) {
        if (!isViewCoordinator())
            return CompletableFuture.failedFuture(
                    new IllegalStateException("forceLeaderElection() must be called on the view coordinator"));

        // Only update the fields if the thread is not running.
        // Calling start will not initiate a second round, it will return the existent future for the running mechanism.
        if (!isVotingThreadRunning()) {
            log.debug("%s: starting forced election (exclude=%s)", local_addr, exclude);
            TimeService time = raft.timeService();
            electionStart = time.now();
            electionStartNanos = time.nanos();
            stopVoting = false;
        }
        return voting_thread.start(exclude);
    }

    public synchronized BaseElection startVotingThread() {
        if (!isVotingThreadRunning()) {
            log.debug("%s: starting the voting thread", local_addr);
            TimeService time = raft.timeService();
            electionStart = time.now();
            electionStartNanos = time.nanos();
            stopVoting = false;
            voting_thread.start();
        }
        return this;
    }

    protected void sendVoteRequest(long new_term) {
        VoteRequest req = new VoteRequest(new_term);
        log.trace("%s -> all: %s", local_addr, req);
        Message vote_req = new EmptyMessage(null).putHeader(id, req).setFlag(OOB);
        down_prot.down(vote_req);
    }

    // sent reliably, so if a newly joined member drops it, it will get retransmitted
    protected void sendLeaderElectedMessage(Address leader, long term) {
        RaftHeader hdr = new LeaderElected(leader).currTerm(term);
        Message msg = new EmptyMessage(null).putHeader(id, hdr).setFlag(DONT_LOOPBACK);
        log.trace("%s -> all (-self): %s", local_addr, hdr);
        down_prot.down(msg);

        // Record election latency.
        if (electionStart != null) {
            TimeService time = raft.timeService();
            electionEnd = time.now();
            if (metrics != null) {
                metrics.recordElectionLatency(time.interval(electionStartNanos));
            }
        }
    }

    protected void sendVoteResponse(Address dest, long term, long last_log_term, long last_log_index) {
        VoteResponse rsp = new VoteResponse(term, last_log_term, last_log_index);
        Message vote_rsp = new EmptyMessage(dest).putHeader(id, rsp).setFlag(OOB);
        log.trace("%s -> %s: %s", local_addr, dest, rsp);
        down_prot.down(vote_rsp);
    }

    public synchronized BaseElection stopVotingThread() {
        if (isVotingThreadRunning()) {
            log.debug("%s: mark the voting thread to stop", local_addr);
            stopVoting = true;
        }
        return this;
    }

    private void stopVotingThreadInternal() {
        log.debug("%s: stopping the voting thread", local_addr);
        voting_thread.stop();
        votes.reset();
    }
}
