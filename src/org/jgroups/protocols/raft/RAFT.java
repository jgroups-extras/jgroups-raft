package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of the RAFT consensus protocol in JGroups
 * @author Bela Ban
 * @since  3.6
 */
@MBean(description="Implementation of the RAFT consensus protocol")
public class RAFT extends Protocol {
    // todo: when moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short RAFT_ID              = 1024;

    // todo: when moving to JGroups -> add to jg-magic-map.xml
    protected static final short APPEND_ENTRIES_REQ   = 2000;
    protected static final short APPEND_ENTRIES_RSP   = 2001;
    protected static final short REQUEST_VOTE_REQ     = 2002;
    protected static final short REQUEST_VOTE_RSP     = 2003;
    protected static final short INSTALL_SNAPSHOT_REQ = 2004;
    protected static final short INSTALL_SNAPSHOT_RSP = 2005;

    protected static enum Role {Follower, Candidate, Leader}

    static {
        ClassConfigurator.addProtocol(RAFT_ID, RAFT.class);
        ClassConfigurator.add(APPEND_ENTRIES_REQ,   AppendEntriesRequest.class);
        ClassConfigurator.add(APPEND_ENTRIES_RSP,   AppendEntriesResponse.class);
        ClassConfigurator.add(REQUEST_VOTE_REQ,     RequestVoteRequest.class);
        ClassConfigurator.add(REQUEST_VOTE_RSP,     RequestVoteResponse.class);
        ClassConfigurator.add(INSTALL_SNAPSHOT_REQ, InstallSnapshotRequest.class);
        ClassConfigurator.add(INSTALL_SNAPSHOT_RSP, InstallSnapshotResponse.class);
    }

    @Property(description="Static majority needed to achieve consensus. This means we have to start 5 servers. " +
      "This property will be removed when dynamic cluster membership has been implemented (section 6 of the RAFT paper)", writable=false)
    protected int majority=3;

    @Property(description="Interval (in ms) at which a leader sends out heartbeats")
    protected long heartbeat_interval=30;

    @Property(description="Min election interval (ms)")
    protected long election_min_interval=150;

    @Property(description="Max election interval (ms). The actual election interval is computed as a random value in " +
      "range [election_min_interval..election_max_interval]")
    protected long election_max_interval=300;



    /** The current role (follower, candidate or leader). Every node starts out as a follower */
    @GuardedBy("impl_lock")
    protected RaftImpl          impl=new Follower().raft(this);
    protected final Lock        impl_lock=new ReentrantLock();
    protected volatile View     view;
    protected Address           local_addr;
    protected TimeScheduler     timer;
    protected Future<?>         election_task;

    /** The current leader (can be null) */
    protected volatile Address  leader;

    /** The current term. Incremented when this node becomes a candidate, or set when a higher term is seen */
    @ManagedAttribute(description="The current term")
    protected int               current_term;

    /** The address of the candidate this node voted for in the current term */
    protected Address           voted_for;

    /** Whether a heartbeat has been received before this election timeout kicked in. If false, the follower becomes
     * candidate and starts a new election */
    protected volatile boolean  heartbeat_received=true;

    /** Used to make state changes, e.g. set the current term if a higher term is encountered */
    protected final Lock        lock=new ReentrantLock();


    public Address leader()                   {return leader;}
    public RAFT    leader(Address new_leader) {this.leader=new_leader; return this;}
    public int     currentTerm()              {return current_term;}

    /** Sets the current term if the new term is greater */
    public RAFT    currentTerm(final int new_term)  {
        // return withLockDo(lock, () -> {current_term=new_term; return this;}); // JDK 8
        return withLockDo(lock,new Callable<RAFT>() {
            public RAFT call() throws Exception {
                if(new_term > current_term) current_term=new_term;
                return null;}
        });
    }

    public RAFT createNewTerm() {
        return withLockDo(lock, new Callable<RAFT>() {
            public RAFT call() throws Exception {
                current_term++;
                voted_for=null;
                return RAFT.this;
            }});
    }

    public Log log() {return log;}

    @ManagedAttribute(description="The current role")
    public String role() {
        RaftImpl tmp=impl;
        return tmp.getClass().getSimpleName();
    }


    public void init() throws Exception {
        super.init();
        if(election_min_interval >= election_max_interval)
            throw new Exception("election_min_interval (" + election_min_interval + ") needs to be smaller than " +
                                  "election_max_interval (" + election_max_interval + ")");
        timer=getTransport().getTimer();
        impl.init();
    }

    public void destroy() {
        super.destroy();
    }

    public void start() throws Exception {
        super.start();
    }

    public void stop() {
        super.stop();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                view=(View)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                RaftHeader hdr=(RaftHeader)msg.getHeader(id);
                if(hdr == null)
                    break;
                handleEvent(msg, hdr);
                return null;
            case Event.VIEW_CHANGE:
                view=(View)evt.getArg();
                break;
        }
        return up_prot.up(evt);
    }


    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            RaftHeader hdr=(RaftHeader)msg.getHeader(id);
            if(hdr != null) {
                batch.remove(msg);
                handleEvent(msg, hdr);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void startElectionTimer() {
        withLockDo(lock, new Callable<Void>() {
            public Void call() throws Exception {
                if(election_task == null || election_task.isDone())
                    election_task=timer.scheduleWithDynamicInterval(new ElectionTask());
                return null;
            }
        });
    }

    protected void stopElectionTimer() {
        withLockDo(lock, new Callable<Void>() {
            public Void call() throws Exception {
                if(election_task != null) election_task.cancel(true);
                return null;
            }
        });
    }

    protected void handleEvent(Message msg, RaftHeader hdr) {
        impl_lock.lock();
        try {
            if(hdr instanceof AppendEntriesRequest) {
                AppendEntriesRequest req=(AppendEntriesRequest)hdr;
                impl.handleAppendEntriesRequest(msg.src(),req.term());
            }
            else if(hdr instanceof AppendEntriesResponse) {
                AppendEntriesResponse rsp=(AppendEntriesResponse)hdr;
                impl.handleAppendEntriesResponse(msg.src(),rsp.term());
            }
            else if(hdr instanceof RequestVoteRequest) {
                RequestVoteRequest header=(RequestVoteRequest)hdr;
                impl.handleRequestVoteRequest(msg.src(),header.term());
            }
            else if(hdr instanceof RequestVoteResponse) {
                RequestVoteResponse rsp=(RequestVoteResponse)hdr;
                impl.handleRequestVoteResponse(msg.src(),rsp.term());
            }
            else if(hdr instanceof InstallSnapshotRequest) {
                InstallSnapshotRequest req=(InstallSnapshotRequest)hdr;
                impl.handleInstallSnapshotRequest(msg.src(),req.term());
            }
            else if(hdr instanceof InstallSnapshotResponse) {
                InstallSnapshotResponse rsp=(InstallSnapshotResponse)hdr;
                impl.handleInstallSnapshotResponse(msg.src(),rsp.term());
            }
            else
                log.warn("%s: invalid header %s",local_addr,hdr.getClass().getCanonicalName());
        }
        finally {
            impl_lock.unlock();
        }
    }

    protected void changeRole(Role new_role) {
        final RaftImpl new_impl=new_role == Role.Follower? new Follower(this) : new_role == Role.Candidate? new Candidate(this) : new Leader(this);
        withLockDo(impl_lock, new Callable<Void>() {
            public Void call() throws Exception {
                if(!impl.getClass().equals(new_impl.getClass())) {
                    impl.destroy();
                    new_impl.init();
                    impl=new_impl;
                }
                return null;
            }
        });
    }

    protected static long computeElectionTimeout(long min,long max) {
        long diff=max - min;
        return (int)((Math.random() * 100000) % diff) + min;
    }

    /** Helper method to get rid of all the boilerplate code of lock handling.
     * Will be replaced with lambdas when moving to JDK 8 */
    protected static <T> T withLockDo(Lock lock, Callable<T> action) {
        lock.lock();
        try {
            try {
                return action.call();
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
        finally {
            lock.unlock();
        }
    }



    protected class ElectionTask implements TimeScheduler.Task {

        public long nextInterval() {
            return computeElectionTimeout(election_min_interval, election_max_interval);
        }

        public void run() {
            withLockDo(impl_lock, new Callable<Void>() {
                public Void call() throws Exception {
                    impl.electionTimeout();
                    return null;
                }
            });
        }
    }


}
