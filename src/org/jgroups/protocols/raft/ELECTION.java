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
    // If moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short ELECTION_ID    = 520;

    // If moving to JGroups -> add to jg-magic-map.xml
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
    protected Role               role=Role.Follower;
    protected volatile View      view;
    protected final Runner       voting_thread=new Runner("voting-thread", this::runVotingProcess, null);

    protected ResponseCollector<VoteResponse> votes=new ResponseCollector<>();

    public long     voteTimeout() {return vote_timeout;}
    public RAFT     raft()        {return raft;}
    public ELECTION raft(RAFT r)  {raft=r; return this;}

    public void resetStats() {
        super.resetStats();
        num_voting_rounds=0;
    }

    @ManagedAttribute(description="The current role")
    public Role role() {return role;}

    @ManagedAttribute(description="Is the voting thread (only on the coordinator) running?")
    public boolean isVotingThreadRunning() {return voting_thread.isRunning();}

    public void init() throws Exception {
        super.init();
        raft=findProtocol(RAFT.class);
    }

    public void stop() {
        stopVotingThread();
        changeRole(Role.Follower);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.DISCONNECT:
                changeRole(Role.Follower);
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
                Address leader=raft.leader();
                if(Objects.equals(leader, local_addr) && has_new_members)
                    sendLeaderElectedMessage(leader, raft.currentTerm());
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
                changeRole(Role.Follower);
                raft.leader(null);
                break;
        }
    }


    protected void handleMessage(Message msg, RaftHeader hdr) {
        if(hdr instanceof LeaderElected) {
            int term=hdr.currTerm();
            Address leader=((LeaderElected)hdr).leader();
            stopVotingThread(); // only on the coord
            boolean is_leader=Objects.equals(leader, local_addr);
            log.trace("%s <- %s: %s", local_addr, msg.src(), hdr);
            changeRole(is_leader? Role.Leader : Role.Follower);
            raft.leader(leader).currentTerm(term);
            return;
        }

        if(hdr instanceof VoteRequest) {
            handleVoteRequest(msg.src());
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
    protected void handleVoteRequest(Address sender) {
        if(log.isTraceEnabled())
            log.trace("%s <- %s: VoteRequest", local_addr, sender);

        int new_term=raft.createNewTerm(); // increment the current term
        Log log_impl=raft.log();
        if(log_impl == null)
            return;
        int my_last_index=log_impl.lastAppended();
        LogEntry entry=log_impl.get(my_last_index);
        int my_last_term=entry != null? entry.term : 0;
        new_term=Math.max(new_term, my_last_term+1);
        sendVoteResponse(sender, new_term, my_last_term, my_last_index);
    }

    protected synchronized void handleVoteResponse(Address sender, VoteResponse rsp) {
        votes.add(sender, rsp);
    }


    protected void runVotingProcess() {
        votes.reset(view.getMembersRaw());
        num_voting_rounds++;
        long start=System.currentTimeMillis();
        sendVoteRequest();

        // wait for responses from all members or for vote_timeout ms, whichever happens first:
        votes.waitForAllResponses(vote_timeout);
        long time=System.currentTimeMillis()-start;

        int majority=raft.majority();
        if(votes.numberOfValidResponses() >= majority) {
            Address leader=determineLeader();
            int new_term=highestTerm();
            log.trace("%s: collected votes from %s in %d ms (majority=%d) -> leader is %s (new_term=%d)",
                      local_addr, votes.getValidResults(), time, majority, leader, new_term);
            if(Objects.equals(local_addr, leader))
                changeRole(Role.Leader);
            sendLeaderElectedMessage(leader, new_term);
            raft.leader(leader).currentTerm(new_term);
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

    protected int highestTerm() {
        Optional<Integer> highest_term=votes.getResults().values().stream().filter(Objects::nonNull)
          .map(RaftHeader::currTerm).max(Integer::compare);
        return highest_term.orElse(0);
    }


    /** Returns true if last_term greater than my own term, false if smaller. If they're equal, returns true if
     *  last_index is > my own last index, false otherwise
     */
    protected boolean isHigher(int last_term, int last_index) {
        int my_last_index=raft.log().lastAppended();
        LogEntry entry=raft.log().get(my_last_index);
        int my_last_term=entry != null? entry.term : 0;
        if(last_term > my_last_term)
            return true;
        if(last_term < my_last_term)
            return false;
        return last_index > my_last_index;
    }

    protected void sendVoteResponse(int term) {
        int last_log_index=raft.log().lastAppended();
        LogEntry entry=raft.log().get(last_log_index);
        int last_log_term=entry != null? entry.term() : 0;
        VoteResponse rsp=new VoteResponse(term, last_log_term, last_log_index);
        log.trace("%s -> all (-self): %s", local_addr, rsp);
        Message vote_req=new EmptyMessage(null).putHeader(id, rsp).setFlag(OOB);
        down_prot.down(vote_req);
    }

    protected void sendVoteRequest() {
        VoteRequest req=new VoteRequest();
        log.trace("%s -> all (-self): %s", local_addr, req);
        Message vote_req=new EmptyMessage(null).putHeader(id, req).setFlag(OOB);
        down_prot.down(vote_req);
    }

    protected void sendVoteResponse(Address dest, int term, int last_log_term, int last_log_index) {
        VoteResponse rsp=new VoteResponse(term, last_log_term, last_log_index);
        Message vote_rsp=new EmptyMessage(dest).putHeader(id, rsp).setFlag(OOB);
        down_prot.down(vote_rsp);
    }

    // sent reliably, so if a newly joined member drops it, it will get retransmitted
    protected void sendLeaderElectedMessage(Address leader, int term) {
        RaftHeader hdr=new LeaderElected(leader).currTerm(term);
        Message msg=new EmptyMessage(null).putHeader(id, hdr).setFlag(DONT_LOOPBACK);
        log.trace("%s -> all (-self): %s", local_addr, hdr);
        down_prot.down(msg);
    }

    protected void changeRole(Role new_role) {
        if(role == new_role) // no change
            return;
        log.debug("%s: changing from %s -> %s", local_addr, role, new_role);
        switch(role) {
            case Follower: // change to leader
                raft.leader(local_addr);
                break;
            case Leader: // change to follower
                raft.leader(null);
                break;
        }
        raft.changeRole(role=new_role);
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
