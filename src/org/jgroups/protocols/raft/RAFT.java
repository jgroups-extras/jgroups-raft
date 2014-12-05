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
import org.jgroups.util.CompletableFuture;
import org.jgroups.util.Function;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Implementation of the RAFT consensus protocol in JGroups
 * @author Bela Ban
 * @since  0.1
 */
@MBean(description="Implementation of the RAFT consensus protocol")
public class RAFT extends Protocol {
    // When moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short  RAFT_ID              = 1024;

    // When moving to JGroups -> add to jg-magic-map.xml
    protected static final short  APPEND_ENTRIES_REQ   = 2000;
    protected static final short  APPEND_ENTRIES_RSP   = 2001;
    protected static final short  INSTALL_SNAPSHOT_REQ = 2002;
    protected static final short  INSTALL_SNAPSHOT_RSP = 2003;
    protected static final short  APPEND_RESULT        = 2004;

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
    protected String            log_class="org.jgroups.protocols.raft.MapDBLog";

    @Property(description="Arguments to the log impl, e.g. k1=v1,k2=v2. These will be passed to init()")
    protected String            log_args;

    @Property(description="The name of the log")
    protected String            log_name="raft.log";

    protected StateMachine      state_machine;

    protected Log               log_impl;



    /** The current role (follower, candidate or leader). Every node starts out as a follower */
    @GuardedBy("impl_lock")
    protected volatile RaftImpl impl=new Follower(this);
    protected volatile View     view;
    protected Address           local_addr;

    /** The current leader (can be null) */
    protected volatile Address  leader;

    /** The current term. Incremented when this node becomes a candidate, or set when a higher term is seen */
    @ManagedAttribute(description="The current term")
    protected int               current_term;

    @ManagedAttribute(description="Index of the highest log entry applied to the state machine")
    protected int               last_applied;

    @ManagedAttribute(description="Index of the highest committed log entry")
    protected int               commit_index;



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
    public synchronized RAFT currentTerm(final int new_term)  {
        if(new_term > current_term)
            current_term=new_term;
        return this;
    }

    @ManagedAttribute(description="The current role")
    public String role() {
        RaftImpl tmp=impl;
        return tmp.getClass().getSimpleName();
    }

    public synchronized int createNewTerm() {
        return ++current_term;
    }

    public synchronized boolean updateTermAndLeader(int term, Address new_leader) {
        if(leader == null || (new_leader != null && !leader.equals(new_leader)))
            leader=new_leader;
        if(term > current_term) {
            current_term=term;
            return true;
        }
        return false;
    }

    public void init() throws Exception {
        super.init();
        if(log_class == null)
            throw new IllegalStateException("log_class has to be defined");
        Class<? extends Log> clazz=Util.loadClass(log_class,getClass());
        log_impl=clazz.newInstance();
        Map<String,String> args;
        if(log_args != null && !log_args.isEmpty())
            args=Util.parseCommaDelimitedProps(log_args);
        else
            args=new HashMap<>();
        log_impl.init(log_name, args);
        last_applied=log_impl.lastApplied();
        commit_index=log_impl.commitIndex();
        log.debug("initialized last_applied=%d and commit_index=%d from log", last_applied, commit_index);
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
     * protocol to get a majority to commit this change.<p/>
     * Only applicable on the leader
     * @param buf The command
     * @param offset The offset into the buffer
     * @param length The length of the buffer
     * @param completion_handler A function that will get called upon completion (async). Can be null
     * @return A CompletableFuture. Can be used to wait for the result (sync). A blocking caller could call
     *         set(), then call future.get() to block for the result.
     */
    public CompletableFuture<Boolean> set(byte[] buf, int offset, int length, Function<Boolean,Void> completion_handler) {
        //if(leader == null || (local_addr != null && !leader.equals(local_addr)))
          //  throw new RuntimeException("I'm not the leader (local_addr=" + local_addr + ", leader=" + leader + ")");

        CompletableFuture<Boolean> retval=new CompletableFuture<>(completion_handler);

        AppendResult result=log_impl.append(last_applied, current_term, new LogEntry(current_term, buf, offset, length));
        if(!result.success) {
            retval.completeExceptionally(new IllegalStateException("log could not be appended to: result=" + result));
            return retval;
        }
        last_applied++; // todo: add to commit table

        // 1. Append to the log

        // 2. Multicast an AppendEntries message

        // 3. Return CompletableFuture

        // 4. [async] Update the RPCs table with responses -> move commitIndex in the log
        // 4.1. Apply committed entries to the state machine

        // 5. [async] Periodically resend data to members whose indices are behind mine



        return retval;
    }


    protected void handleEvent(Message msg, RaftHeader hdr) {
        // log.trace("%s: received %s from %s", local_addr, hdr, msg.src());

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

    protected void changeRole(Role new_role) {
        RaftImpl new_impl=new_role == Role.Follower? new Follower(this) : new_role == Role.Candidate? new Candidate(this) : new Leader(this);
        RaftImpl old_impl=impl;
        if(old_impl == null || !old_impl.getClass().equals(new_impl.getClass())) {
            if(old_impl != null)
                old_impl.destroy();
            new_impl.init();
            synchronized(this) {
                impl=new_impl;
            }
            log.trace("%s: changed role from %s -> %s",local_addr,old_impl == null? "null" :
              old_impl.getClass().getSimpleName(),new_impl.getClass().getSimpleName());
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

    /**
     * Keeps track of next_index and match_index for each cluster member. Used to (1) compute the commit_index and
     * (2) to resend log entries to members which haven't yet seen them.<p/>
     * Only created on the leader
     */
    protected static class CommitTable {
        protected final Map<Address,?> map=new ConcurrentHashMap<>();
    }

}
