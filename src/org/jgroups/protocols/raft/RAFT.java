package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


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
    protected String            log_class="org.jgroups.protocols.raft.LevelDBLog";

    @Property(description="Arguments to the log impl, e.g. k1=v1,k2=v2. These will be passed to init()")
    protected String            log_args;

    @Property(description="The name of the log")
    protected String            log_name; // ="raft.log";

    protected StateMachine      state_machine;

    protected Log               log_impl;

    protected RequestTable      request_table;
    protected CommitTable       commit_table;



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
        /*if(log_class == null)
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
        log.debug("initialized last_applied=%d and commit_index=%d from log", last_applied, commit_index);*/
    }

    @Override
    public void start() throws Exception {
        super.start();
        if(log_class == null)
            throw new IllegalStateException("log_class has to be defined");
        Class<? extends Log> clazz=Util.loadClass(log_class,getClass());
        log_impl=clazz.newInstance();
        Map<String,String> args;
        if(log_args != null && !log_args.isEmpty())
            args=Util.parseCommaDelimitedProps(log_args);
        else
            args=new HashMap<>();

        if(log_name == null) {
            JChannel ch=stack.getChannel();
            log_name=ch != null? ch.getName() : "raft";
        }
        log_name=createLogName(log_name);

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
     * The blocking equivalent of {@link #setAsync(byte[],int,int,org.jgroups.util.Function)}. Used to apply a change
     * across all cluster nodes via consensus.
     * @param buf The serialized command to be applied (interpreted by the caller)
     * @param offset The offset into the buffer
     * @param length The length of the buffer
     * @return The serialized result (to be interpreted by the caller)
     * @throws Exception ExecutionException or InterruptedException
     */
    public byte[] set(byte[] buf, int offset, int length) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length, null);
        return future.get();
    }

    /**
     * Time bounded blocking get(). Returns when the result is available, or a timeout (or exception) has occurred
     * @param buf The buffer
     * @param offset The offset into the buffer
     * @param length The length of the buffer
     * @param timeout The timeout
     * @param unit The unit of the timeout
     * @return The serialized result
     * @throws Exception ExecutionException when the execution failed, InterruptedException, or TimeoutException when
     * the timeout elapsed without getting an exception.
     */
    public byte[] set(byte[] buf, int offset, int length, long timeout, TimeUnit unit) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length, null);
        return future.get(timeout, unit);
    }

    /**
     * Called by a building block to apply a change to all state machines in a cluster. This starts the consensus
     * protocol to get a majority to commit this change.<p/>
     * This call is non-blocking and returns a future as soon as the AppendEntries message has been sent.<p/>
     * Only applicable on the leader
     * @param buf The command
     * @param offset The offset into the buffer
     * @param length The length of the buffer
     * @param completion_handler A function that will get called with the serialized result (may be null)
     *                           upon completion (async). Can be null.
     * @return A CompletableFuture. Can be used to wait for the result (sync). A blocking caller could call
     *         set(), then call future.get() to block for the result.
     */
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Function<byte[],Void> completion_handler) {
        if(leader == null || (local_addr != null && !leader.equals(local_addr)))
            throw new IllegalStateException("I'm not the leader (local_addr=" + local_addr + ", leader=" + leader + ")");
        if(buf == null)
            throw new IllegalArgumentException("buffer must not be null");

        CompletableFuture<byte[]> retval=new CompletableFuture<>(completion_handler);
        int prev_index=0, curr_index=0, prev_term=0, curr_term=0, commit_idx=0;

        RequestTable reqtab=request_table;
        if(reqtab == null) {
            retval.completeExceptionally(new IllegalStateException("request table was null on " + impl.getClass().getSimpleName()));
            return retval;
        }

        // 1. Append to the log
        synchronized(this) {
            prev_index=last_applied;
            curr_index=++last_applied;
            LogEntry entry=log_impl.get(prev_index);
            prev_term=entry != null? entry.term : current_term;
            curr_term=current_term;
            commit_idx=commit_index;
        }

        log_impl.append(curr_index, true, new LogEntry(curr_term, buf, offset, length));

        // 2. Add the request to the client table, so we can return results to clients when done
        reqtab.create(curr_index, local_addr, retval);

        // 3. Multicast an AppendEntries message (exclude self)
        Message msg=new Message(null, buf, offset, length)
          .putHeader(id, new AppendEntriesRequest(curr_term, this.local_addr, prev_index, prev_term, commit_idx))
          .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        down_prot.down(new Event(Event.MSG, msg));

        // 4. Return CompletableFuture
        return retval;

        // 5. [async] Update the RPCs table with responses -> move commitIndex in the log
        // 5.1. Apply committed entries to the state machine
        // 5.2. Return results to the clients

        // 6. [async] Periodically resend data to members whose indices are behind mine
    }


    protected void handleEvent(Message msg, RaftHeader hdr) {
        // log.trace("%s: received %s from %s", local_addr, hdr, msg.src());
        if(hdr instanceof AppendEntriesRequest) {
            AppendEntriesRequest req=(AppendEntriesRequest)hdr;
            impl.handleAppendEntriesRequest(msg.getRawBuffer(), msg.getOffset(), msg.getLength(), msg.src(),
                                            req.term(), req.prev_log_index, req.prev_log_term, req.leader_commit);
        }
        else if(hdr instanceof AppendEntriesResponse) {
            AppendEntriesResponse rsp=(AppendEntriesResponse)hdr;
            impl.handleAppendEntriesResponse(msg.src(),rsp.term(), rsp.result);
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


    /**
     * Received a majority of votes for the entry at index. Note that indices may be received out of order, e.g. if
     * we have modifications at indixes 4, 5 and 6, entry[5] might get a majority of votes (=committed)
     * before entry[3] and entry[6].<p/>
     * The following things are done:
     * <ul>
     *     <li>See if commit_index can be moved to index (incr commit_index until a non-committed entry is encountered)</li>
     *     <li>For each committed entry, apply the modification in entry[index] to the state machine</li>
     *     <li>For each committed entry, notify the client and set the result (CompletableFuture)</li>
     * </ul>
     *
     * @param index The index of the committed entry.
     */
    protected synchronized void handleCommit(int index) {
        try {
            for(int i=commit_index + 1; i <= index; i++) {
                if(request_table.isCommitted(i)) {
                    applyCommit(i);
                    commit_index++;
                }
            }
        }
        catch(Throwable t) {
            log.error("failed applying commit %d", index);
        }
    }

    /** Applies the commit at index */
    protected void applyCommit(int index) throws Exception {
        // Apply the modifications to the state machine
        LogEntry log_entry=log_impl.get(index);
        if(log_entry == null)
            throw new IllegalStateException("log entry for index " + index + " not found in log");
        state_machine.apply(log_entry.command, log_entry.offset, log_entry.length);

        // Notify the client's CompletableFuture and then remove the entry in the client request table
        request_table.notifyAndRemove(index, log_entry.command, log_entry.offset, log_entry.length);

        log_impl.commitIndex(index);
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

    // Replace with Util equivalent when switching to JGroups 3.6.2 or when merging this code into JGroups
    public static <T extends Streamable> void write(T[] array, DataOutput out) throws Exception {
        Bits.writeInt(array != null? array.length : 0, out);
        if(array == null)
            return;
        for(T el: array)
            el.writeTo(out);
    }

    // Replace with Util equivalent when switching to JGroups 3.6.2 or when merging this code into JGroups
    public static <T extends Streamable> T[] read(Class<T> clazz, DataInput in) throws Exception {
        int size=Bits.readInt(in);
        if(size == 0)
            return null;
        T[] retval=(T[])Array.newInstance(clazz, size);

        for(int i=0; i < retval.length; i++) {
            retval[i]=clazz.newInstance();
            retval[i].readFrom(in);
        }
        return retval;
    }

    protected static String createLogName(String name) {
        boolean needs_suffix=!name.endsWith(".log");
        String retval=name;
        if(!new File(name).isAbsolute()) {
            String dir=Util.checkForMac()? File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");
            retval=dir + File.separator + name;
        }
        return needs_suffix? retval + ".log" : retval;
    }


    /**
     * Keeps track of AppendRequest messages and responses. Each AppendEntry request is keyed by the index at which
     * it was inserted at the leader. The values (RequestEntry) contain the responses from followers. When a response
     * is added, and the majority has been reached, add() retuns true and the key/value pair will be removed.
     * (subsequent responses will be ignored). On a majority, the commitIndex is advanced.
     * <p/>
     * Only created on leader
     */
    protected static class RequestTable {
        protected static class Entry {
            // the future has been returned to the caller, and needs to be notified when we've reached a majority
            protected final CompletableFuture<byte[]> client_future;
            protected final Set<Address>              votes=new HashSet<>(); // todo: replace with bitset
            protected boolean                         committed;

            public Entry(CompletableFuture<byte[]> client_future, Address vote) {
                this.client_future=client_future;
                votes.add(vote);
            }

            protected boolean add(Address vote, int majority) {
                boolean reached_majority=votes.add(vote) && votes.size() >= majority;
                return reached_majority && !committed && (committed=true);
            }

            @Override
            public String toString() {
                return "committed=" + committed + ", votes=" + votes;
            }
        }

        // protected final View view; // majority computed as view.size()/2+1
        protected final int                majority;

        // maps an index to a set of (response) senders
        protected final Map<Integer,Entry> requests=new HashMap<>();

        public RequestTable(int majority) {
            this.majority=majority;
        }

        /** Whether or not the entry at index is committed */
        public synchronized boolean isCommitted(int index) {
            Entry entry=requests.get(index);
            return entry != null && entry.committed;
        }

        protected synchronized void create(int index, Address vote, CompletableFuture<byte[]> future) {
            Entry entry=new Entry(future, vote);
            requests.put(index, entry);
        }

        /**
         * Adds a response to the response set. If the majority has been reached, returns true
         * @return True if a majority has been reached, false otherwise. Note that this is done <em>exactly once</em>
         */
        protected synchronized boolean add(int index, Address sender) {
            Entry entry=requests.get(index);
            return entry != null && entry.add(sender, majority);
        }

        /** Notifies the CompletableFuture and then removes the entry for index */
        protected synchronized void notifyAndRemove(int index, byte[] response, int offset, int length) {
            Entry entry=requests.get(index);
            if(entry != null) {
                byte[] value=response;
                if(response != null && offset > 0) {
                    value=new byte[length];
                    System.arraycopy(response, offset, value, 0, length);
                }
                entry.client_future.complete(value);
                requests.remove(index);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb=new StringBuilder();
            for(Map.Entry<Integer,Entry> entry: requests.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            return sb.toString();
        }
    }


    /**
     * Keeps track of next_index and match_index for each cluster member (excluding this leader).
     * Used to (1) compute the commit_index and (2) to resend log entries to members which haven't yet seen them.<p/>
     * Only created on the leader
     */
    protected static class CommitTable {

        protected static class Entry {
            protected int next_index;
            protected int match_index;

            public Entry(int next_index, int match_index) {
                this.next_index=next_index;
                this.match_index=match_index;
            }
        }

        protected final Map<Address,Entry> map=new ConcurrentHashMap<>();


        protected void add(Address member) {

        }

        protected void remove(Address member) {

        }

        protected int setNextIndex(Address member, int index) {
            return 0;
        }

        protected int setMatchIndex(Address member, int index) {
            return 0;
        }
    }

}
