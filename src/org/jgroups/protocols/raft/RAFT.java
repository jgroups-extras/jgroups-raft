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
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;

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

    /** The current leader (can be null) */
    protected volatile Address  leader;

    /** The current term. Incremented when this node becomes a candidate, or set when a higher term is seen */
    protected volatile int      current_term;


    @ManagedAttribute(description="The current role")
    public String role() {
        impl_lock.lock();
        try {
            return impl.getClass().toString();
        }
        finally {
            impl_lock.unlock();
        }
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
        RaftImpl new_impl=null;
        switch(new_role) {
            case Follower:
                new_impl=new Follower(this);
                break;
            case Candidate:
                new_impl=new Follower(this);
                break;
            case Leader:
                new_impl=new Leader(this);
                break;
        }
        impl_lock.lock();
        try {
            if(!impl.getClass().equals(new_impl.getClass())) {
                impl.destroy();
                new_impl.init();
                impl=new_impl;
            }
        }
        finally {
            impl_lock.unlock();
        }
    }

    protected static long computeElectionTimeout(long min,long max) {
        long diff=max - min;
        return (int)((Math.random() * 100000) % diff) + min;
    }


}
