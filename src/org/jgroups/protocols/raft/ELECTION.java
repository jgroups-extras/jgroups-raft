package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Performs leader election. Starts an election timer on connect and starts an election when the timer goes off and
 * no heartbeats have been received. Runs a heartbeat task when leader.
 * @author Bela Ban
 * @since  0.1
 */
@MBean(description="Protocol performing leader election according to the RAFT paper")
public class ELECTION extends Protocol {
    // when moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short ELECTION_ID    = 520;

    // When moving to JGroups -> add to jg-magic-map.xml
    protected static final short VOTE_REQ       = 3000;
    protected static final short VOTE_RSP       = 3001;
    protected static final short HEARTBEAT_REQ  = 3002;


    static {
        ClassConfigurator.addProtocol(ELECTION_ID, ELECTION.class);
        ClassConfigurator.add(VOTE_REQ,      VoteRequest.class);
        ClassConfigurator.add(VOTE_RSP,      VoteResponse.class);
        ClassConfigurator.add(HEARTBEAT_REQ, HeartbeatRequest.class);
    }

    @Property(description="Interval (in ms) at which a leader sends out heartbeats")
    protected long              heartbeat_interval=30;

    @Property(description="Min election interval (ms)")
    protected long              election_min_interval=150;

    @Property(description="Max election interval (ms). The actual election interval is computed as a random value in " +
      "range [election_min_interval..election_max_interval]")
    protected long              election_max_interval=300;

    @Property(description="Use views to start/stop elections (https://github.com/belaban/jgroups-raft/issues/20)")
    protected boolean           use_views=true;

    /** The address of the candidate this node voted for in the current term */
    protected Address           voted_for;

    /** Votes collected for me in the current term (if candidate) */
    @ManagedAttribute(description="Number of votes this candidate received in the current term")
    protected int               current_votes;

    @Property(description="No election will ever be started if true; this node will always be a follower. " +
      "Used only for testing and may get removed. Don't use !")
    protected boolean           no_elections;

    /** Whether a heartbeat has been received before this election timeout kicked in. If false, the follower becomes
     * candidate and starts a new election */
    protected volatile boolean  heartbeat_received=true;

    protected RAFT              raft; // direct ref instead of events
    protected TimeScheduler     timer;
    protected Future<?>         election_task;
    protected Future<?>         heartbeat_task;
    protected Role              role=Role.Follower;

    public long     heartbeatInterval()            {return heartbeat_interval;}
    public ELECTION heartbeatInterval(long val)    {heartbeat_interval=val; return this;}
    public long     electionMinInterval()          {return election_min_interval;}
    public ELECTION electionMinInterval(long val)  {election_min_interval=val; return this;}
    public long     electionMaxInterval()          {return election_max_interval;}
    public ELECTION electionMaxInterval(long val)  {election_max_interval=val; return this;}
    public boolean  noElections()                  {return no_elections;}
    public ELECTION noElections(boolean flag)      {no_elections=flag; return this;}
    public ELECTION timer(TimeScheduler ts)        {timer=ts; return this;}
    public boolean  useViews()                     {return use_views;}
    public ELECTION useViews(boolean b)            {use_views=b; return this;}
    public RAFT     raft()                         {return raft;}
    public ELECTION raft(RAFT r)                   {raft=r; return this;}


    @ManagedAttribute(description="The current role")
    public Role role() {return role;}

    @ManagedAttribute(description="Is the heartbeat task running")
    public synchronized boolean isHeartbeatTaskRunning() {
        return heartbeat_task != null && !heartbeat_task.isDone();
    }

    @ManagedAttribute(description="Is the election ttimer running")
    public synchronized boolean isElectionTimerRunning() {return election_task != null && !election_task.isDone();}

    public void init() throws Exception {
        super.init();
        if(heartbeat_interval < 1L)
        	throw new Exception(String.format("heartbeat_interval (%d) must not be below one", heartbeat_interval));
        if(heartbeat_interval >= election_min_interval)
            throw new Exception(String.format("heartbeat_interval (%d) needs to be smaller than " +
                                  "election_min_interval (%d)", heartbeat_interval, election_min_interval));
        if(election_min_interval >= election_max_interval)
            throw new Exception(String.format("election_min_interval (%d) needs to be smaller than " +
                                  "election_max_interval (%d)", election_min_interval, election_max_interval));
        if(timer == null)
            timer=getTransport().getTimer();
        raft=findProtocol(RAFT.class);
    }

    public void stop() {
        stopHeartbeatTimer();
        stopElectionTimer();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                Object retval=down_prot.down(evt); // connect first
                startElectionTimer();
                return retval;
            case Event.DISCONNECT:
                changeRole(Role.Follower);
                stopElectionTimer();
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
            handleEvent(msg, hdr);
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
                handleEvent(msg, hdr);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void handleView(View v) {
        if(use_views) {
            _handleView(v);
            return;
        }
        if(v == null || v.size() >= raft.majority())
            return;
        if(role == Role.Leader)
            changeRole(Role.Candidate);
        else
            raft.leader(null); // follower or candidate
    }

    protected void _handleView(View v) {
        if(v.size() >= raft.majority()) {
            if(raft.leader() == null)
                startElectionTimer();
            else {
                Address leader=raft.leader();
                if(!v.containsMember(leader))
                    startElectionTimer();
                else if(Objects.equals(leader, local_addr))
                    sendHeartbeat(raft.currentTerm(), leader, true); // send a HB to new members
            }
        }
        else {
            if(role == Role.Leader)
                changeRole(Role.Candidate);
            else
                raft.leader(null); // follower or candidate
            stopElectionTimer();
            stopHeartbeatTimer();
        }
    }


    protected void handleEvent(Message msg, RaftHeader hdr) {
        // drop the message if hdr.term < raft.current_term, else accept
        // if hdr.term > raft.current_term -> change to follower
        int rc=raft.currentTerm(hdr.curr_term);
        if(rc < 0)
            return;
        if(rc > 0) { // a new term was set
            if(hdr instanceof HeartbeatRequest) {
                // we only step down if we're getting a HeartbeatRequest from a leader; requests from candidates
                // don't change anything (https://github.com/belaban/jgroups-raft/issues/81)
                changeRole(Role.Follower);
            }
            voteFor(null); // so we can vote again in this term
        }

        if(hdr instanceof HeartbeatRequest) {
            HeartbeatRequest hb=(HeartbeatRequest)hdr;
            handleHeartbeat(hb.currTerm(), hb.leader);
        }
        else if(hdr instanceof VoteRequest) {
            VoteRequest header=(VoteRequest)hdr;
            handleVoteRequest(msg.src(), header.currTerm(), header.lastLogTerm(), header.lastLogIndex());
        }
        else if(hdr instanceof VoteResponse) {
            VoteResponse rsp=(VoteResponse)hdr;
            if(rsp.result())
                handleVoteResponse(rsp.currTerm());
        }
    }


    protected synchronized void handleHeartbeat(int term, Address leader) {
        if(Objects.equals(local_addr, leader))
            return;
        heartbeatReceived(true);
        if(use_views)
            stopElectionTimer();
        if(role != Role.Follower || raft.updateTermAndLeader(term, leader)) {
            changeRole(Role.Follower);
            voteFor(null);
        }
    }

    protected void handleVoteRequest(Address sender, int term, int last_log_term, int last_log_index) {
        if(log.isTraceEnabled())
            log.trace("%s: received VoteRequest from %s: term=%d, my term=%d, last_log_term=%d, last_log_index=%d",
                      local_addr, sender, term, raft.currentTerm(), last_log_term, last_log_index);
        boolean send_vote_rsp=false;
        synchronized(this) {
            if(voteFor(sender)) {
                if(sameOrNewer(last_log_term, last_log_index))
                    send_vote_rsp=true;
                else
                    log.trace("%s: dropped VoteRequest from %s as my log is more up-to-date", local_addr, sender);
            }
            else
                log.trace("%s: already voted for %s in term %d; skipping vote", local_addr, sender, term);
        }
        if(send_vote_rsp)
            sendVoteResponse(sender, term); // raft.current_term);
    }

    protected synchronized void handleVoteResponse(int term) {
        if(role == Role.Candidate && term == raft.current_term) {
            int majority=raft.majority();
            if(++current_votes >= majority) {
                // we've got the majority: become leader
                log.trace("%s: collected %d votes (majority=%d) in term %d -> becoming leader",
                          local_addr, current_votes, majority, term);
                changeRole(Role.Leader);
            }
        }
    }

    protected void handleElectionTimeout() {
        log.trace("%s: election timeout", local_addr);
        switch(role) {
            case Follower:
                changeRole(Role.Candidate);
                startElection();
                break;
            case Candidate:
                startElection();
                break;
        }
    }

    /**
     * Returns true if last_log_term >= my own last log term, or last_log_index >= my own index
     * @param last_log_term
     * @param last_log_index
     */
    protected boolean sameOrNewer(int last_log_term, int last_log_index) {
        int my_last_log_index;
        LogEntry entry=raft.log().get(my_last_log_index=raft.log().lastAppended());
        int my_last_log_term=entry != null? entry.term : 0;
        int comp=Integer.compare(my_last_log_term, last_log_term);
        return comp <= 0 && (comp < 0 || my_last_log_index <= last_log_index);
    }


    protected synchronized boolean heartbeatReceived(final boolean flag) {
        boolean retval=heartbeat_received;
        heartbeat_received=flag;
        return retval;
    }

    protected void sendHeartbeat(int term, Address leader) {
        sendHeartbeat(term, leader, false);
    }

    protected void sendHeartbeat(int term, Address leader, boolean reliable) {
        Message req=new EmptyMessage(null).putHeader(id, new HeartbeatRequest(term, leader))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK).setFlag(Message.Flag.OOB, Message.Flag.NO_FC);
        if(!reliable)
            req.setFlag(Message.Flag.NO_RELIABILITY);
        down_prot.down(req);
    }

    protected void sendVoteRequest(int term) {
        int last_log_index=raft.log().lastAppended();
        LogEntry entry=raft.log().get(last_log_index);
        int last_log_term=entry != null? entry.term() : 0;
        VoteRequest req=new VoteRequest(term, last_log_term, last_log_index);
        log.trace("%s: sending %s", local_addr, req);
        Message vote_req=new EmptyMessage(null).putHeader(id, req)
          .setFlag(Message.Flag.OOB, Message.Flag.NO_RELIABILITY, Message.Flag.NO_FC);
        down_prot.down(vote_req);
    }

    protected void sendVoteResponse(Address dest, int term) {
        VoteResponse rsp=new VoteResponse(term, true);
        log.trace("%s: sending %s",local_addr,rsp);
        Message vote_rsp=new EmptyMessage(dest).putHeader(id, rsp)
          .setFlag(Message.Flag.OOB, Message.Flag.NO_RELIABILITY, Message.Flag.NO_FC);
        down_prot.down(vote_rsp);
    }

    protected void changeRole(Role new_role) {
        if(role == new_role)
            return;
        if(role != Role.Leader && new_role == Role.Leader) {
            raft.leader(local_addr);
            // send a first heartbeat immediately after the election so other candidates step down
            sendHeartbeat(raft.currentTerm(), raft.leader(), use_views);
            stopElectionTimer();
            if(!use_views)
                startHeartbeatTimer();
        }
        else if(role == Role.Leader && new_role != Role.Leader) {
            stopHeartbeatTimer();
            if(!use_views)
                startElectionTimer();
            raft.leader(null);
        }
        role=new_role;
        raft.changeRole(role);
    }

    protected void startElection() {
        int new_term=0;

        synchronized(this) {
            new_term=raft.createNewTerm();
            voteFor(null);
            current_votes=0;
            if(!voteFor(local_addr))
                return;
        }
        // send VoteRequest message; responses are received asynchronously. If majority -> become leader
        sendVoteRequest(new_term);
    }

    @ManagedAttribute(description="Vote cast for a candidate in the current term")
    public synchronized String votedFor() {
        return voted_for != null? voted_for.toString() : null;
    }

    protected boolean voteFor(final Address addr) {
        if(addr == null) {
            voted_for=null;
            return true;
        }
        if(voted_for == null) {
            voted_for=addr;
            return true;
        }
        return voted_for.equals(addr); // a vote for the same candidate in the same term is ok
    }


    public void startElectionTimer() {
        if(!no_elections && (election_task == null || election_task.isDone()))
            election_task=timer.scheduleWithDynamicInterval(new ElectionTask());
    }

    protected void stopElectionTimer() {
        if(election_task != null) election_task.cancel(true);
    }

    protected void startHeartbeatTimer() {
        if(heartbeat_task == null || heartbeat_task.isDone())
            heartbeat_task=timer.scheduleAtFixedRate(new HeartbeatTask(), heartbeat_interval, heartbeat_interval, TimeUnit.MILLISECONDS);
    }

    protected void stopHeartbeatTimer() {
        if(heartbeat_task != null)
            heartbeat_task.cancel(true);
    }

    protected <T extends Protocol> T findProtocol(Class<T> clazz) {
        for(Protocol p=up_prot; p != null; p=p.getUpProtocol()) {
            if(clazz.isAssignableFrom(p.getClass()))
                return (T)p;
        }
        throw new IllegalStateException(clazz.getSimpleName() + " not found above " + this.getClass().getSimpleName());
    }





    protected class ElectionTask implements TimeScheduler.Task {
        public long nextInterval() {
            return computeElectionTimeout(election_min_interval, election_max_interval);
        }

        public void run() {
            if(!heartbeatReceived(false))
                handleElectionTimeout();
        }
        protected long computeElectionTimeout(long min,long max) {
            long diff=max - min;
            return (int)((Math.random() * 100000) % diff) + min;
        }
    }

    protected class HeartbeatTask implements Runnable {
        public void run() {sendHeartbeat(raft.currentTerm(), raft.leader());}
    }

}
