package org.jgroups.protocols.raft.election;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Runner;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

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

    protected static final short VOTE_REQ       = 3000;
    static final short VOTE_RSP                 = 3001;
    static final short LEADER_ELECTED           = 3005;
    protected static final short PRE_VOTE_REQ   = 3006;
    protected static final short PRE_VOTE_RSP   = 3007;

    static {
        ClassConfigurator.add(VOTE_REQ,       VoteRequest.class);
        ClassConfigurator.add(VOTE_RSP,       VoteResponse.class);
        ClassConfigurator.add(LEADER_ELECTED, LeaderElected.class);
    }

    protected RAFT raft;

    private final Runner voting_thread=new Runner("voting-thread", this::runVotingProcess, null);
    private final ResponseCollector<VoteResponse> votes=new ResponseCollector<>();

    protected volatile View view;

    public long                            voteTimeout() {return vote_timeout;}

    /**
     * Defines the default timeout in milliseconds to utilize during any election operation.
     *
     * @param timeoutMs: Timeout value in milliseconds.
     * @return This election instance.
     * @throws IllegalArgumentException: In case timeout is less than or equal to 0.
     */
    public BaseElection voteTimeout(long timeoutMs) {
        if (timeoutMs <= 0) throw new IllegalArgumentException("Timeout should be greater than 0.");

        this.vote_timeout = timeoutMs;
        return this;
    }

    public RAFT                            raft()        {return raft;}
    public BaseElection                    raft(RAFT r)  {raft=r; return this;}
    public ResponseCollector<VoteResponse> getVotes()    {return votes;} // use for testing only!

    @Property(description="Max time (ms) to wait for vote responses",type= AttributeType.TIME)
    protected long               vote_timeout=600;

    @ManagedAttribute(description="Number of voting rounds initiated by the coordinator")
    protected int                num_voting_rounds;

    @ManagedAttribute(description="Is the voting thread (only on the coordinator) running?")
    public synchronized boolean isVotingThreadRunning() {return voting_thread.isRunning();}

    @ManagedOperation(description="Trigger the voting process (only on the coordinator)")
    public boolean runVotingThread() {
        if(isViewCoordinator()) {
            startVotingThread();
            return isVotingThreadRunning();
        }
        return false;
    }

    protected abstract void handleView(View v);

    public void resetStats() {
        super.resetStats();
        num_voting_rounds=0;
    }

    @Override
    public void init() throws Exception {
        super.init();
        raft=RAFT.findProtocol(RAFT.class, this, false);
    }

    @Override
    public void stop() {
        stopVotingThread();
        if (raft != null)
            raft.setLeaderAndTerm(null);
    }

    @Override
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.DISCONNECT:
                raft.setLeaderAndTerm(null);
                break;
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }

    @Override
    public Object up(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return up_prot.up(evt);
    }

    @Override
    public Object up(Message msg) {
        RaftHeader hdr=msg.getHeader(id);
        if(hdr != null) {
            handleMessage(msg, hdr);
            return null;
        }
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        for(Iterator<Message> it = batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            RaftHeader hdr=msg.getHeader(id);
            if(hdr != null) {
                it.remove();
                handleMessage(msg, hdr);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
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
        long term=hdr.currTerm();
        Address leader=hdr.leader();
        View v = this.view;
        // Only receive messages with null view when running tests with mock cluster.
        // Otherwise, need to make sure the leader is the current view and there's still a majority.
        // The view could change between the leader is decided and the message arrives.
        if (v == null || (isLeaderInView(leader, v) && isMajorityAvailable(v, raft))) {
            log.trace("%s <- %s: %s", local_addr, msg.src(), hdr);
            stopVotingThread(); // only on the coord
            raft.setLeaderAndTerm(leader, term); // possibly changes the role
        } else {
            log.trace("%s <- %s: %s after leader left (%s)", local_addr, msg.src(), hdr, v);
        }
    }

    /**
     * Handle a received VoteRequest sent by the coordinator; send back the last log term and index. Restricts execution
     * to a single process per term. Do not reply the request if voted for another process in the same term.
     *
     * @param msg: The received message.
     * @param hdr: The received header.
     */
    private void handleVoteRequest(Message msg, VoteRequest hdr) {
        Address sender = msg.src();
        long new_term = hdr.currTerm();
        if(log.isTraceEnabled())
            log.trace("%s <- %s: VoteRequest(term=%d)", local_addr, sender, new_term);

        int result=raft.currentTerm(new_term);
        switch(result) {
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
        }

        Address voted_for=raft.votedFor();
        if(voted_for != null && !voted_for.equals(sender)) {
            log.trace("%s: already voted (for %s) in term %d; dropping vote request from %s",
                    local_addr, voted_for, new_term, sender);
            return;
        }
        Log log_impl=raft.log();
        if(log_impl == null)
            return;
        raft.votedFor(sender);
        long my_last_index=log_impl.lastAppended();
        LogEntry entry=log_impl.get(my_last_index);
        long my_last_term=entry != null? entry.term() : 0;
        sendVoteResponse(sender, new_term, my_last_term, my_last_index);
    }

    private void handleVoteResponse(Message msg, VoteResponse hdr) {
        votes.add(msg.src(), hdr);
    }

    protected Address determineLeader() {
        Address leader=null;
        VoteResponse higher = null;
        Map<Address,VoteResponse> results=votes.getResults();
        for(Address mbr: view.getMembersRaw()) {
            VoteResponse rsp=results.get(mbr);
            if(rsp == null)
                continue;
            if (isHigher(higher, rsp)) {
                leader = mbr;
                higher = rsp;
            }
        }
        return leader;
    }

    /**
     * Compares the {@link VoteResponse}s in search of the highest one.
     * <p>
     * The verification follows the precedence:
     * <ol>
     *     <li>Compare Raft terms;</li>
     *     <li>Compare the log last appended.</li>
     * </ol>
     * This verification ensures the decided leader has the longest log.
     * </p>
     *
     * @param one: The base to check against.
     * @param other: The candidate response to check.
     * @return <code>true</code> if {@param other} is higher than {@param one}. <code>false</code>, otherwise.
     */
    private boolean isHigher(VoteResponse one, VoteResponse other) {
        if (one == null) return true;

        // The candidate response has a higher Raft term.
        if (one.last_log_term < other.last_log_term)
            return true;

        // The candidate response has an outdated Raft term.
        if (one.last_log_term > other.last_log_term)
            return false;

        // Both responses have the same term.
        // Break ties utilizing the index of the last appended entry.
        return one.last_log_index < other.last_log_index;
    }

    /**
     * The mechanism for deciding a new leader.
     * <p>
     * Increases its term and queries all participants about their term and log index information. The thread blocks
     * (with a timeout) wait for responses. If the majority replies, the process with the highest term and index is elected.
     * <p>
     * The process keeps running until a leader is elected or the majority is lost.
     */
    protected void runVotingProcess() {
        // If the thread is interrupted, means the voting thread was already stopped.
        // We place this here just as a shortcut to not increase the term in RAFT.
        if (Thread.interrupted()) return;

        View electionView = this.view;
        long new_term=raft.createNewTerm();
        votes.reset(electionView.getMembersRaw());
        num_voting_rounds++;
        long start=System.currentTimeMillis();
        sendVoteRequest(new_term);

        // wait for responses from all members or for vote_timeout ms, whichever happens first:
        votes.waitForAllResponses(vote_timeout);
        long time=System.currentTimeMillis()-start;

        int majority=raft.majority();
        if(votes.numberOfValidResponses() >= majority) {
            Address leader=determineLeader();
            log.trace("%s: collected votes from %s in %d ms (majority=%d) -> leader is %s (new_term=%d)",
                    local_addr, votes.getResults(), time, majority, leader, new_term);

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
                    stopVotingThread();
                    raft.setLeaderAndTerm(null);
                    return;
                }

                // At this point, the majority still in place, so we confirm the elected leader is still present in the view.
                // If the leader is not in the view anymore, we keep the voting thread running.
                if (isLeaderInView(leader, view)) {
                    stopVotingThread();
                    return;
                }

                if (log.isTraceEnabled())
                    log.trace("%s: leader (%s) not in view anymore, retrying", local_addr, leader);
            }
        } else if (log.isTraceEnabled())
            log.trace("%s: collected votes from %s in %d ms (majority=%d); starting another voting round",
                    local_addr, votes.getValidResults(), time, majority);
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

    public synchronized BaseElection startVotingThread() {
        if(!isVotingThreadRunning()) {
            log.debug("%s: starting the voting thread", local_addr);
            voting_thread.start();
        }
        return this;
    }

    protected void sendVoteRequest(long new_term) {
        VoteRequest req=new VoteRequest(new_term);
        log.trace("%s -> all: %s", local_addr, req);
        Message vote_req=new EmptyMessage(null).putHeader(id, req).setFlag(OOB);
        down_prot.down(vote_req);
    }

    // sent reliably, so if a newly joined member drops it, it will get retransmitted
    protected void sendLeaderElectedMessage(Address leader, long term) {
        RaftHeader hdr=new LeaderElected(leader).currTerm(term);
        Message msg=new EmptyMessage(null).putHeader(id, hdr).setFlag(DONT_LOOPBACK);
        log.trace("%s -> all (-self): %s", local_addr, hdr);
        down_prot.down(msg);
    }

    protected void sendVoteResponse(Address dest, long term, long last_log_term, long last_log_index) {
        VoteResponse rsp=new VoteResponse(term, last_log_term, last_log_index);
        Message vote_rsp=new EmptyMessage(dest).putHeader(id, rsp).setFlag(OOB);
        log.trace("%s -> %s: %s", local_addr, dest, rsp);
        down_prot.down(vote_rsp);
    }

    public synchronized BaseElection stopVotingThread() {
        if(isVotingThreadRunning()) {
            log.debug("%s: stopping the voting thread", local_addr);
            voting_thread.stop();
            votes.reset();
        }
        return this;
    }
}
