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
import org.jgroups.util.Util;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of the RAFT consensus protocol in JGroups
 * @author Bela Ban
 * @since  0.1
 */
@MBean(description="Implementation of the RAFT consensus protocol")
public class RAFT extends Protocol implements Settable {
    // When moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short RAFT_ID              = 1024;

    // When moving to JGroups -> add to jg-magic-map.xml
    protected static final short APPEND_ENTRIES_REQ   = 2000;
    protected static final short APPEND_ENTRIES_RSP   = 2001;
    protected static final short INSTALL_SNAPSHOT_REQ = 2002;
    protected static final short INSTALL_SNAPSHOT_RSP = 2003;
    protected static final short APPEND_RESULT        = 2004;

    static {
        ClassConfigurator.addProtocol(RAFT_ID,      RAFT.class);
        ClassConfigurator.add(APPEND_ENTRIES_REQ,   AppendEntriesRequest.class);
        ClassConfigurator.add(APPEND_ENTRIES_RSP,   AppendEntriesResponse.class);
        ClassConfigurator.add(INSTALL_SNAPSHOT_REQ, InstallSnapshotRequest.class);
        ClassConfigurator.add(INSTALL_SNAPSHOT_RSP, InstallSnapshotResponse.class);
        ClassConfigurator.add(APPEND_RESULT,        AppendResult.class);
    }


    @Property(description="Static majority needed to achieve consensus. This means we have to start " +
      "majority*2-1 servers. This property will be removed when dynamic cluster membership has been " +
      "implemented (section 6 of the RAFT paper)", writable=false)
    protected int               majority=2;


    @Property(description="The fully qualified name of the class implementing Log")
    protected String            log_class;

    @Property(description="Arguments to the log impl, e.g. k1=v1,k2=v2. These will be passed to init()")
    protected String            log_args;

    protected StateMachine      state_machine;

    protected Log               log_impl;



    /** The current role (follower, candidate or leader). Every node starts out as a follower */
    @GuardedBy("impl_lock")
    protected volatile RaftImpl impl=new Follower(this);
    protected final Lock        impl_lock=new ReentrantLock();
    protected volatile View     view;
    protected Address           local_addr;

    /** The current leader (can be null) */
    protected volatile Address  leader;

    /** The current term. Incremented when this node becomes a candidate, or set when a higher term is seen */
    @ManagedAttribute(description="The current term")
    protected int               current_term;


    /** Used to make state changes, e.g. set the current term if a higher term is encountered */
    protected final Lock        lock=new ReentrantLock();

    @ManagedAttribute(description="Current leader")
    public String       getLeader() {return leader != null? leader.toString() : "none";}

    public Address      leader()                      {return leader;}
    public RAFT         leader(Address new_leader)    {this.leader=new_leader; return this;}
    public RAFT         stateMachine(StateMachine sm) {this.state_machine=sm; return this;}
    public StateMachine stateMachine()                {return state_machine;}
    public int          currentTerm()                 {return current_term;}
    public Log          log()                         {return log_impl;}
    public RAFT         log(Log new_log)              {this.log_impl=new_log; return this;}


    /** Sets the current term if the new term is greater */
    public RAFT    currentTerm(final int new_term)  {
        // return withLockDo(lock, () -> {current_term=new_term; return this;}); // JDK 8
        return withLockDo(lock,new Callable<RAFT>() {
            public RAFT call() throws Exception {
                if(new_term > current_term) current_term=new_term;
                return null;}
        });
    }

    @ManagedAttribute(description="The current role")
    public String role() {
        RaftImpl tmp=impl;
        return tmp.getClass().getSimpleName();
    }

    public int createNewTerm() {
        return withLockDo(lock, new Callable<Integer>() {
            public Integer call() throws Exception {
                return ++current_term;
            }});
    }

    public boolean updateTermAndLeader(final int term, final Address new_leader) {
        return withLockDo(lock, new Callable<Boolean>() {
            public Boolean call() throws Exception {
                if(leader == null || (new_leader != null && !leader.equals(new_leader)))
                    leader=new_leader;
                if(term > current_term) {
                    current_term=term;
                    return true;
                }
                return false;
            }
        });
    }

    public void init() throws Exception {
        super.init();
        if(log_class != null) {
            Class<? extends Log> clazz=Util.loadClass(log_class,getClass());
            log_impl=clazz.newInstance();
            if(log_args != null && !log_args.isEmpty()) {
                Map<String,String> args=Util.parseCommaDelimitedProps(log_args);
                log_impl.init(args);
            }
        }
    }

    public void stop() {
        super.stop();
        impl.destroy();
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

    /**
     * Called by a building block to apply a change to all state machines in a cluster. This starts the consensus
     * protocol to get a majority to make this change committed.<p/>
     * Only applicable on the leader
     * @param buf The command
     * @param offset The offset into the buffer
     * @param length The length of the buffer
     */
    public void set(byte[] buf, int offset, int length) {
        // Add to log, send an AppendEntries to all nodes, wait for majority, then commit to log and return to client
        if(leader == null || (local_addr != null && !leader.equals(local_addr)))
            throw new RuntimeException("I'm not the leader (local_addr=" + local_addr + ", leader=" + leader + ")");
        // raft_log.append()
    }


    protected void handleEvent(Message msg, RaftHeader hdr) {
        // log.trace("%s: received %s from %s", local_addr, hdr, msg.src());
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
                RaftImpl old_impl=impl;
                if(impl == null || !impl.getClass().equals(new_impl.getClass())) {
                    if(impl != null)
                        impl.destroy();
                    new_impl.init();
                    impl=new_impl;
                    log.trace("%s: changed role from %s -> %s", local_addr,
                              old_impl == null? "null" : old_impl.getClass().getSimpleName(),
                              new_impl.getClass().getSimpleName());
                }
                return null;
            }
        });
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


    public static <T> T findProtocol(Class<T> clazz, final Protocol start, boolean down) {
        Protocol prot=start;
        while(prot != null && clazz != null) {
            if(prot != start && clazz.isAssignableFrom(prot.getClass()))
                return (T)prot;
            prot=down? prot.getDownProtocol() : prot.getUpProtocol();
        }
        return null;
    }

}
