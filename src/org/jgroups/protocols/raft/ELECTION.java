package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.raft.util.Utils;
import org.jgroups.raft.util.Utils.Majority;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Runner;

import java.util.*;

import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

/**
 * Performs leader election. When coordinator, starts a voting thread which asks everyone to cast their votes. Stops
 * when a new leader has been elected.
 * @author Bela Ban
 * @since  0.1
 */
@MBean(description="Protocol performing leader election according to the RAFT paper")
public class ELECTION extends Protocol {
    protected static final short ELECTION_ID    = 520;
    protected static final short VOTE_REQ       = 3000;
    protected static final short VOTE_RSP       = 3001;
    protected static final short LEADER_ELECTED = 3005;

    static {
        ClassConfigurator.addProtocol(ELECTION_ID, ELECTION.class);
        ClassConfigurator.add(VOTE_REQ,      VoteRequest.class);
        ClassConfigurator.add(VOTE_RSP,      VoteResponse.class);
        ClassConfigurator.add(LEADER_ELECTED, LeaderElected.class);
    }

    @Property(description="Max time (ms) to wait for vote responses",type=AttributeType.TIME)
    protected long               vote_timeout=600;
    @ManagedAttribute(description="Number of voting rounds initiated by the coordinator")
    protected int                num_voting_rounds;

    protected RAFT               raft; // direct ref instead of events
    protected volatile View      view;
    protected final Runner       voting_thread=new Runner("voting-thread", this::runVotingProcess, null);

    protected ResponseCollector<VoteResponse> votes=new ResponseCollector<>();

    public long                            voteTimeout() {return vote_timeout;}
    public RAFT                            raft()        {return raft;}
    public ELECTION                        raft(RAFT r)  {raft=r; return this;}
    public ResponseCollector<VoteResponse> getVotes()    {return votes;} // use for testing only!

    public void resetStats() {
        super.resetStats();
        num_voting_rounds=0;
    }

    @ManagedAttribute(description="Is the voting thread (only on the coordinator) running?")
    public boolean isVotingThreadRunning() {return voting_thread.isRunning();}

    public void init() throws Exception {
        super.init();
        raft=findProtocol(RAFT.class);
    }

    public void stop() {
        stopVotingThread();
        raft.setLeaderAndTerm(null);
    }

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

    public Object up(Message msg) {
        RaftHeader hdr=msg.getHeader(id);
        if(hdr != null) {
            handleMessage(msg, hdr);
            return null;
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
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


    protected void handleView(View v) {
        Majority result=Utils.computeMajority(view, v, raft().majority(), raft.leader());
        log.debug("%s: existing view: %s, new view: %s, result: %s", local_addr, this.view, v, result);
        List<Address> joiners=View.newMembers(this.view, v);
        boolean has_new_members=joiners != null && !joiners.isEmpty();
        this.view=v;
        switch(result) {
            case no_change: // the leader resends its term/address for new members to set the term/leader
                if(raft.isLeader() && has_new_members)
                    sendLeaderElectedMessage(raft.leader(), raft.currentTerm());
                break;
            case reached:
            case leader_lost:
                if(Objects.equals(this.view.getCoord(), local_addr)) {
                    log.trace("%s: starting voting process (reason: %s, view: %s)", local_addr, result, view);
                    startVotingThread();
                }
                break;
            case lost:
                stopVotingThread(); // if running, double-dutch
                raft.setLeaderAndTerm(null);
                break;
        }
    }


    protected void handleMessage(Message msg, RaftHeader hdr) {
        if(hdr instanceof LeaderElected) {
            long term=hdr.currTerm();
            Address leader=((LeaderElected)hdr).leader();
            stopVotingThread(); // only on the coord
            log.trace("%s <- %s: %s", local_addr, msg.src(), hdr);
            raft.setLeaderAndTerm(leader, term); // possibly changes the role
            return;
        }
        if(hdr instanceof VoteRequest) {
            handleVoteRequest(msg.src(), hdr.currTerm());
            return;
        }
        if(hdr instanceof VoteResponse) {
            VoteResponse rsp=(VoteResponse)hdr;
            log.trace("%s <- %s: %s", local_addr, msg.src(), hdr);
            handleVoteResponse(msg.src(), rsp);
        }
    }

    /**
     * Handle a received VoteRequest sent by the coordinator; send back the last log term and index
     * @param sender The sender of the VoteRequest
     */
    protected void handleVoteRequest(Address sender, long new_term) {
        if(log.isTraceEnabled())
            log.trace("%s <- %s: VoteRequest(term=%d)", local_addr, sender, new_term);

        int result=raft.currentTerm(new_term);
        switch(result) {
            case -1: // new_term < current_term
                log.trace("%s: received vote request from %s with term=%d (current_term=%d); dropping vote response",
                         local_addr, sender, new_term, raft.currentTerm());
                return;
            case 1: // new_term > current_term
                raft.votedFor(null);
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
        long my_last_term=entry != null? entry.term : 0;
        sendVoteResponse(sender, new_term, my_last_term, my_last_index);
    }

    protected void handleVoteResponse(Address sender, VoteResponse rsp) {
        votes.add(sender, rsp);
    }


    protected void runVotingProcess() {
        long new_term=raft.createNewTerm();
        raft.votedFor(null);
        votes.reset(view.getMembersRaw());
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
                      local_addr, votes.getValidResults(), time, majority, leader, new_term);
            sendLeaderElectedMessage(leader, new_term); // send to all - self
            raft.setLeaderAndTerm(leader, new_term);
            stopVotingThread();
        }
        else
            log.trace("%s: collected votes from %s in %d ms (majority=%d); starting another voting round",
                      local_addr, votes.getValidResults(), time, majority);
    }

    protected Address determineLeader() {
        Address leader=null;
        Map<Address,VoteResponse> results=votes.getResults();
        for(Address mbr: view.getMembersRaw()) {
            VoteResponse rsp=results.get(mbr);
            if(rsp == null)
                continue;
            if(leader == null)
                leader=mbr;
            if(isHigher(rsp.last_log_term, rsp.last_log_index))
                leader=mbr;
        }
        return leader;
    }

    protected Long highestTerm() {
        Optional<Long> highest_term=votes.getResults().values().stream().filter(Objects::nonNull)
          .map(RaftHeader::currTerm).max(Long::compare);
        return highest_term.orElse(0L);
    }


    /** Returns true if last_term greater than my own term, false if smaller. If they're equal, returns true if
     *  last_index is > my own last index, false otherwise
     */
    protected boolean isHigher(long last_term, long last_index) {
        long my_last_index=raft.log().lastAppended();
        LogEntry entry=raft.log().get(my_last_index);
        long my_last_term=entry != null? entry.term : 0;
        if(last_term > my_last_term)
            return true;
        if(last_term < my_last_term)
            return false;
        return last_index > my_last_index;
    }

    protected void sendVoteResponse(long term) {
        long last_log_index=raft.log().lastAppended();
        LogEntry entry=raft.log().get(last_log_index);
        long last_log_term=entry != null? entry.term() : 0;
        VoteResponse rsp=new VoteResponse(term, last_log_term, last_log_index);
        log.trace("%s -> all (-self): %s", local_addr, rsp);
        Message vote_req=new EmptyMessage(null).putHeader(id, rsp).setFlag(OOB);
        down_prot.down(vote_req);
    }

    protected void sendVoteRequest(long new_term) {
        VoteRequest req=new VoteRequest(new_term);
        log.trace("%s -> all: %s", local_addr, req);
        Message vote_req=new EmptyMessage(null).putHeader(id, req).setFlag(OOB);
        down_prot.down(vote_req);
    }

    protected void sendVoteResponse(Address dest, long term, long last_log_term, long last_log_index) {
        VoteResponse rsp=new VoteResponse(term, last_log_term, last_log_index);
        Message vote_rsp=new EmptyMessage(dest).putHeader(id, rsp).setFlag(OOB);
        down_prot.down(vote_rsp);
    }

    // sent reliably, so if a newly joined member drops it, it will get retransmitted
    protected void sendLeaderElectedMessage(Address leader, long term) {
        RaftHeader hdr=new LeaderElected(leader).currTerm(term);
        Message msg=new EmptyMessage(null).putHeader(id, hdr).setFlag(DONT_LOOPBACK);
        log.trace("%s -> all (-self): %s", local_addr, hdr);
        down_prot.down(msg);
    }

    public synchronized ELECTION startVotingThread() {
        if(!isVotingThreadRunning())
            voting_thread.start();
        return this;
    }

    public synchronized ELECTION stopVotingThread() {
        if(isVotingThreadRunning()) {
            log.debug("%s: stopping the voting thread", local_addr);
            voting_thread.stop();
            votes.reset();
        }
        return this;
    }

    protected <T extends Protocol> T findProtocol(Class<T> clazz) {
        for(Protocol p=up_prot; p != null; p=p.getUpProtocol()) {
            if(clazz.isAssignableFrom(p.getClass()))
                return (T)p;
        }
        throw new IllegalStateException(clazz.getSimpleName() + " not found above " + this.getClass().getSimpleName());
    }


    protected static long computeElectionTimeout(long min, long max) {
        long diff=max - min;
        return (int)((Math.random() * 100000) % diff) + min;
    }


}
