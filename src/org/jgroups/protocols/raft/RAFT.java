package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.EmptyMessage;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.raft.state.RaftState;
import org.jgroups.raft.Options;
import org.jgroups.raft.Settable;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.LogCache;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Runner;
import org.jgroups.util.Util;

import java.io.File;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Implementation of the <a href="https://github.com/ongardie/dissertation">RAFT consensus protocol</a> in JGroups<p/>
 * [1] https://github.com/ongardie/dissertation<br/>
 * The implementation uses a queue to which the following types of requests are added: down-requests (invocations of
 * {@link #setAsync(byte[], int, int)})
 * and up-requests (requests or responses received in {@link #up(Message)} or {@link #up(MessageBatch)}).
 * <br/>
 * Leaders handle down-requests (resulting in sending AppendEntriesRequests) and up-requests (responses).
 * Followers handle only up-requests (AppendEntriesRequests) and send responses. Note that the periodic sending of
 * AppendEntriesRequests (if needed) is also done by the queue handling thread.
 * <br/>
 * The use of the queue makes the RAFT protocol effectively <em>single-threaded</em>; ie. only 1 thread ever changes
 * state, so synchronization can be removed altogether. The only exception to this is invocation of
 * {@link #changeRole(Role)}, called by {@link ELECTION}: this still needs to be changed (probably by adding it as an
 * event to the queue, too).
 * @author Bela Ban
 * @since  0.1
 */
@MBean(description="Implementation of the RAFT consensus protocol")
public class RAFT extends Protocol implements Settable, DynamicMembership {
    public static final byte[]    raft_id_key          = Util.stringToBytes("raft-id");
    protected static final short  RAFT_ID              = 521;
    protected static final short  APPEND_ENTRIES_REQ   = 2000;
    protected static final short  APPEND_ENTRIES_RSP   = 2001;
    protected static final short  APPEND_RESULT        = 2002;
    protected static final short  INSTALL_SNAPSHOT_REQ = 2003;
    protected static final short  LOG_ENTRIES          = 2004;

    public static final Function<ExtendedUUID,String> print_function=uuid -> {
        byte[] val=uuid.get(raft_id_key);
        return val != null? Util.bytesToString(val) : uuid.print();
    };

    static {
        ClassConfigurator.addProtocol(RAFT_ID, RAFT.class);
        ClassConfigurator.add(APPEND_ENTRIES_REQ,   AppendEntriesRequest.class);
        ClassConfigurator.add(APPEND_ENTRIES_RSP,   AppendEntriesResponse.class);
        ClassConfigurator.add(APPEND_RESULT,        AppendResult.class);
        ClassConfigurator.add(INSTALL_SNAPSHOT_REQ, InstallSnapshotRequest.class);
        ClassConfigurator.add(LOG_ENTRIES,          LogEntries.class);
    }

    @Property(description="The identifier of this node. Needs to be unique and an element of members. Must not be null",
              writable=false)
    protected String                    raft_id;

    protected final PersistentState     internal_state=new PersistentState();
    protected final RaftState           raft_state = new RaftState(this, this::leaderUpdated);

    @ManagedAttribute(description="Majority needed to achieve consensus; computed from members)")
    protected int                       majority=-1;

    @Property(description="If true, we can change 'members' at runtime")
    protected boolean                   dynamic_view_changes=true;

    @Property(description="The fully qualified name of the class implementing Log")
    protected String                    log_class="org.jgroups.protocols.raft.LevelDBLog";

    @Property(description="Arguments to the log impl, e.g. k1=v1,k2=v2. These will be passed to init()")
    protected String                    log_args;

    @Property(description="The directory in which the log and snapshots are stored. Defaults to the temp dir")
    protected String                    log_dir=Util.checkForMac()?
      File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");

    @Property(description="The prefix of the log and snapshot. If null, the logical name of the channel is used as prefix")
    protected String                    log_prefix;

    @ManagedAttribute(description="The name of the log")
    protected String                    log_name;

    @Property(description="Interval (ms) at which AppendEntries messages are resent to members with missing log entries",
      type=AttributeType.TIME)
    protected long                      resend_interval=1000;

    @Property(description="Send commit message to followers immediately after leader commits (majority has consensus). " +
      "Caution : it may generate more traffic than expected")
    protected boolean                   send_commits_immediately;

    @Property(description="Max number of bytes a log can have until a snapshot is created",type=AttributeType.BYTES)
    protected int                       max_log_size=1_000_000;

    protected int                       _max_log_cache_size=1024;

    protected boolean                   _log_use_fsync;

    @ManagedAttribute(description="The current size of the log in bytes",type=AttributeType.BYTES)
    protected long                      curr_log_size; // keeps counts of the bytes added to the log

    @ManagedAttribute(description="Number of successful AppendEntriesRequests")
    protected int                       num_successful_append_requests;

    @ManagedAttribute(description="Number of snapshot messages received (by a follower)")
    protected int                       num_snapshot_received;

    @ManagedAttribute(description="Average AppendEntries batch size")
    protected AverageMinMax             avg_append_entries_batch_size=new AverageMinMax();

    @ManagedAttribute(description="Number of failed AppendEntriesRequests because the entry wasn't found in the log")
    protected int                       num_failed_append_requests_not_found;

    @ManagedAttribute(description="Number of failed AppendEntriesRequests because the prev entry's term didn't match")
    protected int                       num_failed_append_requests_wrong_term;

    protected StateMachine              state_machine;

    protected boolean                   state_machine_loaded;

    protected Log                       log_impl;

    protected RequestTable<String>      request_table;
    protected CommitTable               commit_table;

    protected final List<RoleChange>    role_change_listeners=new ArrayList<>();

    // Set to true during an addServer()/removeServer() op until the change has been committed
    // protected final AtomicBoolean       members_being_changed = new AtomicBoolean(false);

    /** The current role (follower, candidate or leader). Every node starts out as a follower */
    protected volatile RaftImpl         impl=new Follower(this);
    protected volatile View             view;

    @ManagedAttribute(description="Index of the highest log entry appended to the log",type=AttributeType.SCALAR)
    protected long                      last_appended;

    @ManagedAttribute(description="Index of the last committed log entry",type=AttributeType.SCALAR)
    protected long                      commit_index;

    @ManagedAttribute(description="The number of snapshots performed")
    protected int                       num_snapshots;

    @ManagedAttribute(description="The number of times AppendEntriesRequests were resent")
    protected int                       num_resends;

    @Property(description="Max size in items the processing queue can have",type=AttributeType.SCALAR)
    protected int                       processing_queue_max_size=9182;

    /** All requests are added to this queue; a single thread processes this queue - hence no synchronization issues */
    protected BlockingQueue<Request>    processing_queue;

    protected final List<Request>       remove_queue=new ArrayList<>();

    protected Runner                    runner; // the single thread processing the request queue

    protected boolean                   synchronous; // used by the synchronous execution framework (only for testing)

    // used to add/remove servers one-by-one
    protected CompletableFuture<byte[]> add_server_future=CompletableFuture.completedFuture(null);

    /* ============================== EXPERIMENTAL - most of these metrics will be removed again ================== */

    @ManagedAttribute(description="Size of remove-queue")
    public int removeQueueSize() {
        return remove_queue.size();
    }

    @ManagedAttribute(description="Size of processing queue")
    public int processingQueueSize() {
        return processing_queue.size();
    }

    @ManagedAttribute
    final LongAdder     drained_total=new LongAdder();

    @ManagedAttribute
    final AverageMinMax drained_avg=new AverageMinMax();

    @ManagedAttribute
    final LongAdder     drained_down=new LongAdder(), drained_up=new LongAdder();

    @ManagedAttribute  public String drainRatio() {
        double down=(double)drained_down.sum() / drained_total.sum();
        double up=(double)drained_up.sum() / drained_total.sum();
        return String.format("down=%.2f up=%.2f", down, up);
    }
    /* ============================================================================================================ */


    public String       raftId()                      {return raft_id;}
    public RAFT         raftId(String id)             {if(id != null) this.raft_id=id; return this;}
    public RaftImpl     impl()                        {return impl;}
    public int          majority()                    {return majority;}
    public String       logClass()                    {return log_class;}
    public RAFT         logClass(String clazz)        {log_class=clazz; return this;}
    public String       logArgs()                     {return log_args;}
    public RAFT         logArgs(String args)          {log_args=args; return this;}
    public String       logPrefix()                   {return log_prefix;}
    public RAFT         logPrefix(String name)        {log_prefix=name; return this;}
    public String       logName()                     {return log_name;}
    public long         resendInterval()              {return resend_interval;}
    public RAFT         resendInterval(long val)      {resend_interval=val; return this;}
    public boolean      sendCommitsImmediately()      {return send_commits_immediately;}
    public RAFT         sendCommitsImmediately(boolean v) {send_commits_immediately=v; return this;}
    public int          maxLogSize()                  {return max_log_size;}
    public RAFT         maxLogSize(int val)           {max_log_size=val; return this;}
    public long         currentLogSize()              {return curr_log_size;}
    @ManagedAttribute(description="Number of pending requests")
    public int          requestTableSize()            {return request_table != null? request_table.size() : 0;}
    public int          numSnapshots()                {return num_snapshots;}

    @ManagedAttribute(description="The current leader (can be null if there is currently no leader) ")
    public Address      leader()                      {return raft_state.leader();}
    public RAFT         leader(Address new_leader)    {this.raft_state.setLeader(new_leader); return this;}
    public boolean      isLeader()                    {return Objects.equals(leader(), local_addr);}
    public RAFT         stateMachine(StateMachine sm) {this.state_machine=sm; return this;}
    public StateMachine stateMachine()                {return state_machine;}
    public CommitTable  commitTable()                 {return commit_table;}

    @ManagedAttribute(description="The current term. Incremented on leader change, or when a higher term is seen")
    public long         currentTerm()                 {return raft_state.currentTerm();}

    @ManagedAttribute(description="The member this member voted for in the current term")
    public Address      votedFor()                    {return raft_state.votedFor();}
    public long         lastAppended()                {return last_appended;}
    public long         commitIndex()                 {return commit_index;}
    public Log          log()                         {return log_impl;}
    public RAFT         log(Log new_log)              {this.log_impl=new_log; return this;}
    public RAFT         addRoleListener(RoleChange c) {this.role_change_listeners.add(c); return this;}
    public RAFT         remRoleListener(RoleChange c) {this.role_change_listeners.remove(c); return this;}
    public RAFT         stateMachineLoaded(boolean b) {this.state_machine_loaded=b; return this;}
    public boolean      synchronous()                 {return synchronous;}
    public RAFT         synchronous(boolean b)        {synchronous=b; return this;}

    public RAFT logDir(String logDir) {
        this.log_dir = logDir;
        return this;
    }

    public void resetStats() {
        super.resetStats();
        num_snapshots=num_resends=num_successful_append_requests=num_failed_append_requests_not_found
          =num_failed_append_requests_wrong_term=num_snapshot_received=0;
        if(log_impl instanceof LogCache)
            ((LogCache)log_impl).resetStats();
        drained_total.reset(); drained_avg.clear(); drained_down.reset(); drained_up.reset();
        avg_append_entries_batch_size.clear();
    }

    @Property(description="Max size of the log cache (0 disables the log cache)",type=AttributeType.BYTES)
    public int maxLogCacheSize() {
        return _max_log_cache_size;
    }

    @Property
    public RAFT maxLogCacheSize(int size) {
        _max_log_cache_size=size;
        if(log_impl == null) // initial configuration
            return this;
        if(log_impl instanceof LogCache)
            ((LogCache)log_impl).maxSize(size);
        else {
            if(size <= 0)
                disableLogCache();
            else
                enableLogCache();
        }
        return this;
    }

    @Property(description="If true, a change is guaranteed to be written to disk when the call returns")
    public RAFT    logUseFsync(boolean b) {_log_use_fsync=b; if(log_impl != null) log_impl.useFsync(b); return this;}
    @Property
    public boolean logUseFsync()          {return log_impl.useFsync();}

    @ManagedAttribute(description="Number of times the log cache has been trimmed",type=AttributeType.SCALAR)
    public int logCacheNumTrims() {
        return log_impl instanceof LogCache? ((LogCache)log_impl).numTrims() : 0;
    }

    @ManagedAttribute(description="Number of times the cache has been accessed",type=AttributeType.SCALAR)
    public int LogCacheNumAccesses() {
        return log_impl instanceof LogCache? ((LogCache)log_impl).numAccesses() : 0;
    }

    @ManagedAttribute(description="Hit ratio of the cache")
    public double logCacheHitRatio() {
        return log_impl instanceof LogCache? ((LogCache)log_impl).hitRatio() : 0;
    }

    @Property(description="List of members (logical names); majority is computed from it")
    public void setMembers(String list) {
        members(Util.parseCommaDelimitedStrings(list));
    }

    public RAFT members(Collection<String> list) {
        internal_state.setMembers(list);
        computeMajority();
        return this;
    }

    @ManagedAttribute(description = "The current list of members")
    public List<String> members() {
        return internal_state.getMembers();
    }

    /**
     * Sets current_term if new_term is bigger
     * @param new_term The new term
     * @return -1 if new_term is smaller, 0 if equal and 1 if new_term is bigger
     */
    public int currentTerm(final long new_term)  {
        return raft_state.tryAdvanceTerm(new_term);
    }

    public RAFT votedFor(Address mbr) {
        raft_state.setVotedFor(mbr);
        return this;
    }

    @ManagedAttribute(description="The current role")
    public String role()            {return impl.getClass().getSimpleName();}

    @ManagedOperation(description="Dumps the commit table")
    public String dumpCommitTable() {return commit_table != null? "\n" + commit_table : "n/a";}

    @ManagedAttribute(description="Number of log entries in the log")
    public long logSize()           {return log_impl.size();}

    @ManagedAttribute(description="Describes the log")
    public String logDescription() {
        if(log_impl instanceof LogCache) {
            LogCache lc=(LogCache)log_impl;
            return String.format("%s (%d/%d) -> %s", lc.getClass().getSimpleName(), lc.cacheSize(), lc.maxSize(),
                                 lc.log().getClass().getSimpleName());

        }
        return log_impl.getClass().getSimpleName();
    }


    /** This is a managed operation because it should invoked sparingly (costly) */
    @ManagedOperation(description="Number of bytes in the log")
    public long logSizeInBytes() {
        return log_impl.sizeInBytes();
    }

    @ManagedOperation(description="Dumps the last N log entries")
    public String dumpLog(long last_n) {
        final StringBuilder sb=new StringBuilder();
        long to=last_appended, from=Math.max(1, to-last_n);
        log_impl.forEach((entry,index) ->
                           sb.append("index=").append(index).append(", term=").append(entry.term()).append(" (")
                             .append(entry.command().length).append(" bytes)\n"),
                         from, to);
        return sb.toString();
    }

    @ManagedOperation(description="Dumps all log entries")
    public String dumpLog() {return dumpLog(last_appended - 1);}

    @ManagedOperation(description="Enabled the log cache")
    public void enableLogCache() {
        if(!(log_impl instanceof LogCache)) {
            if(_max_log_cache_size <= 0)
                log.error("cannot enable log cache as max_log_cache_size is 0");
            else
                log_impl=new LogCache(log_impl, _max_log_cache_size);
        }
    }

    @ManagedOperation(description="Disables the log cache")
    public void disableLogCache() {
        if(log_impl instanceof LogCache) {
            LogCache lc=(LogCache)log_impl;
            log_impl=lc.log();
            lc.clear();
        }
    }

    @ManagedOperation(description="Clears the log cache")
    public RAFT clearLogCache() {
        if(log_impl instanceof LogCache)
            ((LogCache)log_impl).clear();
        return this;
    }

    @ManagedOperation(description="Trims the log cache to max_log_cache_size")
    public RAFT trimLogCache() {
        if(log_impl instanceof LogCache)
            ((LogCache)log_impl).trim();
        return this;
    }

    public void logEntries(ObjLongConsumer<LogEntry> func) {
        log_impl.forEach(func);
    }

    public long createNewTerm() {
        return raft_state.advanceTermForElection();
    }

    @SuppressWarnings("unchecked")
    public static <T> T findProtocol(Class<T> clazz, final Protocol start, boolean down) {
        Protocol prot=start;
        while(prot != null && clazz != null) {
            if(clazz.isAssignableFrom(prot.getClass()))
                return (T)prot;
            prot=down? prot.getDownProtocol() : prot.getUpProtocol();
        }
        return null;
    }

    @ManagedOperation(description="Adds a new server to members. Prevents duplicates")
    public CompletableFuture<byte[]> addServer(String name) throws Exception {
        return changeMembers(name, InternalCommand.Type.addServer);
    }

    @ManagedOperation(description="Removes a new server from members")
    public CompletableFuture<byte[]> removeServer(String name) throws Exception {
        return changeMembers(name, InternalCommand.Type.removeServer);
    }

    /** Creates a snapshot and truncates the log. See https://github.com/belaban/jgroups-raft/issues/7 for details */
    @ManagedOperation(description="Creates a new snapshot and truncates the log")
    public void snapshot() throws Exception {
        snapshotAsync().get();
    }

    public CompletableFuture<Void> snapshotAsync() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        offer(new SnapshotRequest(f)); return f;
    }

    /** Loads the log entries from [first .. commit_index] into the state machine */
    @ManagedOperation(description="Reads the snapshot (if present) and loads log entries from [first .. commit_index] " +
      "into the state machine")
    public void initStateMachineFromLog() throws Exception {
        if(state_machine == null || state_machine_loaded)
            return;
        int snapshot_offset=0;  // 0 when no snapshot is present, 1 otherwise
        ByteBuffer sn=log_impl.getSnapshot();
        if(sn != null) {
            ByteArrayDataInputStream in=new ByteArrayDataInputStream(sn);
            internal_state.readFrom(in);
            state_machine.readContentFrom(in);
            snapshot_offset=1;
            log.debug("%s: initialized state machine from snapshot (%d bytes)", local_addr, sn.position());
        }

        long from=Math.max(1, log_impl.firstAppended()+snapshot_offset), to=commit_index, count=0;
        for(long i=from; i <= to; i++) {
            LogEntry log_entry=log_impl.get(i);
            if(log_entry == null) {
                log.error("%s: log entry for index %d not found in log", local_addr, i);
                break;
            }
            if(log_entry.command != null) {
                if(log_entry.internal)
                    executeInternalCommand(null, log_entry.command, log_entry.offset, log_entry.length);
                else {
                    state_machine.apply(log_entry.command, log_entry.offset, log_entry.length, true);
                    count++;
                }
            }
        }
        state_machine_loaded=true;
        if(count > 0)
            log.debug("%s: applied %d entries from the log (%d - %d) to the state machine", local_addr, count, from, to);
    }

    @Override public void init() throws Exception {
        super.init();
        // we can only add/remove 1 member at a time (section 4.1 of [1])
        Set<String> tmp=new HashSet<>(internal_state.getMembers());
        if(tmp.size() != internal_state.getMembers().size()) {
            log.error("members (%s) contains duplicates; removing them and setting members to %s", internal_state.getMembers(), tmp);
            internal_state.setMembers(new ArrayList<>(tmp));
        }
        computeMajority();
        if(raft_id == null)
            raft_id=InetAddress.getLocalHost().getHostName();

        // Set an AddressGenerator in channel which generates ExtendedUUIDs and adds the raft_id to the hashmap
        JChannel ch=stack != null? stack.getChannel() : null;
        if(ch != null) {
            ch.addAddressGenerator(() -> {
                ExtendedUUID.setPrintFunction(print_function);
                return ExtendedUUID.randomUUID(ch.getName()).put(raft_id_key, Util.stringToBytes(raft_id));
            });
        }
        processing_queue=new ArrayBlockingQueue<>(processing_queue_max_size);
        runner=new Runner(new DefaultThreadFactory("runner", true, true),
                          "runner", this::processQueue, null);
    }

    @Override public void start() throws Exception {
        super.start();

        if(log_impl == null) {
            if(log_class == null)
                throw new IllegalStateException("log_class has to be defined");
            Class<?> clazz=Util.loadClass(log_class, getClass());
            log_impl=(Log)clazz.getDeclaredConstructor().newInstance();
            Map<String,String> args;
            if(log_args != null && !log_args.isEmpty())
                args=parseCommaDelimitedProps(log_args);
            else
                args=new HashMap<>();

            if(log_prefix == null)
                log_prefix=raft_id;
            log_name=createLogName(log_prefix, "log");
            log_impl.init(log_name, args);
        }

        if(!(local_addr instanceof ExtendedUUID))
            throw new IllegalStateException("local address must be an ExtendedUUID but is a " + local_addr.getClass().getSimpleName());

        last_appended=log_impl.lastAppended();
        commit_index=log_impl.commitIndex();
        raft_state.reload();
        log.trace("%s: set last_appended=%d, commit_index=%d, current_state=%s", local_addr, last_appended, commit_index, raft_state);

        initStateMachineFromLog();
        if(!internal_state.getMembers().contains(raft_id))
            throw new IllegalStateException(String.format("raft-id %s is not listed in members %s", raft_id, internal_state.getMembers()));

        curr_log_size=logSizeInBytes();
        log_impl.useFsync(_log_use_fsync);

        if(_max_log_cache_size > 0)  // the log cache is enabled
            log_impl=new LogCache(log_impl, _max_log_cache_size);
        runner.start();
    }


    @Override public void stop() {
        super.stop();
        add_server_future.complete(null);
        runner.stop();
        impl.destroy();
        Util.close(log_impl);
    }


    public Object down(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        RaftHeader hdr=msg.getHeader(id);
        if(hdr != null) {
            if(synchronous)
                handleUpRequest(msg, hdr);
            else
                add(new UpRequest(msg, hdr));
            return null;
        }
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it = batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            RaftHeader hdr=msg.getHeader(id);
            if(hdr != null) {
                it.remove();
                if(synchronous)
                    handleUpRequest(msg, hdr);
                else
                    add(new UpRequest(msg, hdr));
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    @ManagedOperation(description="Sends all pending AppendEntriesRequests")
    public void flushCommitTable() {
        if(commit_table != null)
            commit_table.forEach(this::sendAppendEntriesMessage);
    }

    /**
     * Triggers a flush of the entries to the given member.
     *
     * @param member: The not-null member address to send the entries.
     * @throws IllegalStateException: Thrown in case the current node is <b>not</b> the leader.
     * @throws NullPointerException: Thrown in case the {@param member} is null.
     */
    public void flushCommitTable(Address member) {
        if (!isLeader()) throw new IllegalStateException("Currently not the leader, should be " + leader());
        CommitTable.Entry e=commit_table.get(Objects.requireNonNull(member));
        if(e != null)
            sendAppendEntriesMessage(member, e);
    }


    /**
     * Called by a building block to apply a change to all state machines in a cluster. This starts the consensus
     * protocol to get a majority to commit this change.<p/>
     * This call is non-blocking and returns a future as soon as the AppendEntries message has been sent.<p/>
     * Only applicable on the leader
     * @param buf The command
     * @param offset The offset into the buffer
     * @param length The length of the buffer
     * @return A CompletableFuture. Can be used to wait for the result (sync). A blocking caller could call
     *         set(), then call future.get() to block for the result.
     */
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Options options) {
        return setAsync(buf, offset, length, false, options);
    }

    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, boolean internal, Options options) {
        Address leader = leader();
        if(leader == null || (local_addr != null && !leader.equals(local_addr)))
            throw notCurrentLeader();
        if(buf == null)
            throw new IllegalArgumentException("buffer must not be null");
        CompletableFuture<byte[]> retval=new CompletableFuture<>();
        RequestTable<String> reqtab=request_table;
        if(reqtab == null) {
            retval.completeExceptionally(new IllegalStateException("request table was null on " + impl.getClass().getSimpleName()));
            return retval;
        }
        if(synchronous) // set only for testing purposes
            handleDownRequest(retval, buf, offset, length, internal, options);
        else {
            offer(new DownRequest(retval, buf, offset, length, internal, options)); // will call handleDownRequest()
        }
        return retval; // 4. Return CompletableFuture
    }

    public String toString() {
        return String.format("%s %s: commit=%d last-appended=%d curr-state=%s",
                             RAFT.class.getSimpleName(), local_addr, commit_index, last_appended, raft_state);
    }

    protected void add(Request r) {
        try {
            processing_queue.put(r);
        }
        catch(InterruptedException ex) {
            log.error("%s: failed adding %s to processing queue: %s", local_addr, r, ex);
            r.failed(ex);
        }
    }

    protected void offer(Request r) {
        if (!processing_queue.offer(r)) {
            r.failed(new IllegalStateException("processing queue is full"));
        }
    }

    /** This method is always called by a single thread only, and does therefore not need to be reentrant */
    protected void handleDownRequest(CompletableFuture<byte[]> f, byte[] buf, int offset, int length,
                                     boolean internal, Options opts) {
        Address leader = leader();
        if(leader == null || !Objects.equals(leader,local_addr))
            throw notCurrentLeader();

        RequestTable<String> reqtab=request_table;

        // 1. Append to the log
        long prev_index=last_appended;
        long curr_index=++last_appended;
        long current_term=currentTerm();
        LogEntry entry=log_impl.get(prev_index);
        long prev_term=entry != null? entry.term : 0;
        LogEntries entries=new LogEntries().add(new LogEntry(current_term, buf, offset, length, internal));
        last_appended=log_impl.append(curr_index, entries);
        num_successful_append_requests+=entries.size();

        // 2. Add the request to the client table, so we can return results to clients when done
        reqtab.create(curr_index, raft_id, f, this::majority, opts);

        // 3. Multicast an AppendEntries message (exclude self)
        Message msg=new ObjectMessage(null, entries)
          .putHeader(id, new AppendEntriesRequest(this.local_addr, current_term, prev_index, prev_term,
                                                  current_term, commit_index))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        down_prot.down(msg);

        snapshotIfNeeded(length);

        // see if we can already commit some entries
        long highest_committed=prev_index+1;
        while(reqtab.isCommitted(highest_committed))
            highest_committed++;
        if(highest_committed > prev_index+1)
            commitLogTo(highest_committed, true);
    }

    public void handleUpRequest(Message msg, RaftHeader hdr) {
        // if hdr.term < current_term -> drop message
        // if hdr.term > current_term -> set current_term and become Follower, accept message
        // if hdr.term == current_term -> accept message
        int rc=currentTerm(hdr.curr_term);
        if(rc < 0)
            return;

        // same term (rc == 0)
        RaftImpl ri=impl;
        if(ri == null)
            return;

        if(hdr instanceof AppendEntriesRequest) {
            long current_term = currentTerm();
            AppendEntriesRequest r=(AppendEntriesRequest)hdr;
            ObjectMessage om=(ObjectMessage)msg;
            log.trace("%s: from %s, %s header %s", local_addr, msg.src(), om, r);
            AppendResult res=ri.handleAppendEntriesRequest(om.getObject(), msg.src(),
                                                           r.prev_log_index, r.prev_log_term, r.entry_term,
                                                           r.leader_commit);
            res.commitIndex(commit_index);
            Message rsp=new EmptyMessage(msg.src()).putHeader(id, new AppendEntriesResponse(current_term, res));
            down_prot.down(rsp);
        }
        else if(hdr instanceof AppendEntriesResponse) {
            AppendEntriesResponse rsp=(AppendEntriesResponse)hdr;
            log.trace("%s: from %s res %s", local_addr, msg.src(), rsp);
            ri.handleAppendEntriesResponse(msg.src(),rsp.curr_term, rsp.result);
        }
        else if(hdr instanceof InstallSnapshotRequest) {
            InstallSnapshotRequest req=(InstallSnapshotRequest)hdr;
            ri.handleInstallSnapshotRequest(msg, req.leader, req.last_included_index, req.last_included_term);
        }
        else
            log.warn("%s: invalid header %s",local_addr,hdr.getClass().getCanonicalName());
    }

    protected void processQueue() {
        Request first_req;
        try {
            first_req=processing_queue.poll(resend_interval, TimeUnit.MILLISECONDS);
            if(first_req == null) { // poll() timed out
                if(commit_table != null)
                    commit_table.forEach(this::sendAppendEntriesMessage);
                return;
            }
            for(;;) {
                remove_queue.clear();
                if(first_req != null) {
                    remove_queue.add(first_req);
                    first_req=null;
                }
                processing_queue.drainTo(remove_queue);
                int num=remove_queue.size();
                if(num > 0) {
                    drained_total.add(num);
                    drained_avg.add(num);

                    final AtomicInteger down_r=new AtomicInteger(), up_r=new AtomicInteger();
                    remove_queue.forEach(r -> {
                        if(r instanceof DownRequest)
                            down_r.incrementAndGet();
                        else if(r instanceof UpRequest)
                            up_r.incrementAndGet();
                    });
                    drained_down.add(down_r.get());
                    drained_up.add(up_r.get());
                }
                if(remove_queue.isEmpty())
                    return;
                else
                    process(remove_queue);
            }
        }
        catch(InterruptedException ignored) {
        }
    }

    protected void process(List<Request> q) {
        RequestTable<String> reqtab=request_table;
        LogEntries           entries=new LogEntries();
        long                 index=last_appended+1;
        int                  length=0;
        long                 current_term = currentTerm();
        Address              leader = leader();

        for(Request r: q) {
            try {
                if(r instanceof UpRequest) {
                    UpRequest up=(UpRequest)r;
                    handleUpRequest(up.msg, up.hdr);
                }
                else if(r instanceof DownRequest) {
                    DownRequest dr=(DownRequest)r;
                    // Complete the request exceptionally.
                    // The request could either be lost in the reqtab reference or fail with an NPE below.
                    // It would only complete in case a timeout is associated.
                    if (!isLeader()) {
                        dr.f.completeExceptionally(notCurrentLeader());
                        continue;
                    }

                    entries.add(new LogEntry(current_term, dr.buf, dr.offset, dr.length, dr.internal));

                    // Add the request to the client table, so we can return results to clients when done
                    reqtab.create(index++, raft_id, dr.f, this::majority, dr.options);
                    length+=dr.length;
                }
                else if (r instanceof SnapshotRequest) {
                    SnapshotRequest sr = (SnapshotRequest) r;
                    try {
                        takeSnapshot(); sr.f.complete(null);
                    } catch (Exception e) {
                        sr.f.completeExceptionally(e);
                        throw e;
                    }
                }
            }
            catch(Throwable ex) {
                log.error("%s: failed handling request %s: %s", local_addr, r, ex);
            }
        }

        if(entries.size() == 0)
            return;

        // handle down requests
        if(leader == null || !Objects.equals(leader,local_addr))
            throw notCurrentLeader();

        // Append to the log
        long prev_index=last_appended;
        long curr_index=last_appended+1;
        LogEntry entry=log_impl.get(prev_index);
        long prev_term=entry != null? entry.term : 0;

        // Multicast an AppendEntries message (exclude self)
        Message msg=new ObjectMessage(null, entries)
          .putHeader(id, new AppendEntriesRequest(this.local_addr, current_term, prev_index, prev_term,
                                                  current_term, commit_index))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        down_prot.down(msg);

        // Appends entries to my own log
        last_appended=log_impl.append(curr_index, entries);
        int batch_size=entries.size();
        num_successful_append_requests+=batch_size;
        avg_append_entries_batch_size.add(batch_size);

        // see if we can already commit some entries
        long highest_committed=prev_index+1;
        while(reqtab.isCommitted(highest_committed))
            highest_committed++;
        if(highest_committed > prev_index+1)
            commitLogTo(highest_committed, true);
        snapshotIfNeeded(length);
    }

    IllegalStateException notCurrentLeader() {
        return new IllegalStateException("I'm not the leader (local_addr=" + local_addr + ", leader=" + leader() + ")");
    }

    /** Populate with non-committed entries (from log) (https://github.com/belaban/jgroups-raft/issues/31) */
    protected void createRequestTable() {
        request_table=new RequestTable<>();
        for(long i=this.commit_index+1; i <= this.last_appended; i++)
            request_table.create(i, raft_id, null, this::majority);
    }

    protected void createCommitTable() {
        List<Address> jg_mbrs=view != null? view.getMembers() : new ArrayList<>();
        List<Address> mbrs=new ArrayList<>(jg_mbrs);
        mbrs.remove(local_addr);
        commit_table=new CommitTable(mbrs, last_appended +1);
    }

    protected void _addServer(String name) {
        if(name == null) return;
        List<String> current= internal_state.getMembers();
        if(!current.contains(name)) {
            current.add(name);
            internal_state.setMembers(current);
            computeMajority();
        }
    }

    protected void _removeServer(String name) {
        if(name == null) return;
        List<String> current= internal_state.getMembers();
        if(current.remove(name)) {
            internal_state.setMembers(current);
            computeMajority();
        }
    }

    /**
     * Runs (on the leader) as part of the queue handling loop: checks if all members (except the leader) in the commit
     * table have received all messages and resends AppendEntries messages to members who haven't.<br/>
     * For each member, a next-index and match-index is maintained: next-index is the index of the next message to send to
     * that member (initialized to last-applied) and match-index is the index of the highest message known to have
     * been received by the member.<br/>
     * Messages are resent to a given member as long as that member's match-index is smaller than its next-index. When
     * match_index == next_index, message resending for that member is stopped. When a new message is sent,
     * next-index is incremented (on reception of the AppendResult) and resending starts again.
     */
    protected void sendAppendEntriesMessage(Address member, CommitTable.Entry e) {
        if(e.nextIndex() < log().firstAppended()) {
            try {
                sendSnapshotTo(member); // will reset snapshot_in_progress
            }
            catch(Exception ex) {
                log.error("%s: failed sending snapshot to %s: next_index=%d, first_applied=%d",
                          local_addr, member, e.nextIndex(), log().firstAppended());
            }
            return;
        }
        if(this.last_appended >= e.nextIndex()) {
            long to=e.sendSingleMessage()? e.nextIndex() : last_appended;
            long from=Math.max(e.nextIndex(),1);
            if(log.isTraceEnabled())
                log.trace("%s: resending [%d..%d] to %s", local_addr, from, to, member);
            resend(member, from, to);
            return;
        }
        if(this.last_appended > e.matchIndex()) {
            long index=this.last_appended;
            if(index > 0) {
                log.trace("%s: resending %d to %s", local_addr, index, member);
                resend(member, index);
            }
            return;
        }
        if(this.commit_index > e.commitIndex()) { // send an empty AppendEntries message as commit message
            long current_term = currentTerm();
            Message msg=new ObjectMessage(member, null)
              .putHeader(id, new AppendEntriesRequest(this.local_addr, current_term, 0, 0,
                                                      current_term, this.commit_index));
            down_prot.down(msg);
            return;
        }
        if(this.commit_index < this.last_appended) // fixes https://github.com/belaban/jgroups-raft/issues/30
            resend(member, this.commit_index+1, this.last_appended);
    }

    protected CompletableFuture<byte[]> changeMembers(String name, InternalCommand.Type type) throws Exception {
        if(!dynamic_view_changes)
            throw new Exception("dynamic view changes are not allowed; set dynamic_view_changes to true to enable it");

        Address leader = leader();
        if(leader == null || !Objects.equals(leader, local_addr))
            throw notCurrentLeader();

        InternalCommand cmd=new InternalCommand(type, name);
        byte[] buf=Util.streamableToByteBuffer(cmd);

        // only add/remove one server at a time (https://github.com/belaban/jgroups-raft/issues/175)
        return add_server_future=add_server_future
                // Use handle, so we can execute even if the previous execution failed.
                .handle((ignore, t) -> setAsync(buf, 0, buf.length, true, null))
                // Chain the new setAsync invocation.
                .thenCompose(Function.identity());
    }

    protected void resend(Address target, long index) {
        LogEntry entry=log_impl.get(index);
        if(entry == null) {
            log.error("%s: resending of %d failed; entry not found", local_addr, index);
            return;
        }
        LogEntry prev=log_impl.get(index-1);
        long prev_term=prev != null? prev.term : 0;
        LogEntries entries=new LogEntries().add(entry);
        Message msg=new ObjectMessage(target, entries)
          .putHeader(id, new AppendEntriesRequest(this.local_addr, currentTerm(), index - 1, prev_term,
                                                  entry.term, commit_index));
        down_prot.down(msg);
        num_resends++;
    }

    /** Resends all entries in range [from .. to] to target */
    protected void resend(Address target, long from, long to) {
        LogEntries entries=new LogEntries();
        long entry_term=0; // term of first entry to resend
        for(long i=from; i <= to; i++) {
            LogEntry e=log_impl.get(i);
            if(e == null) {
                log.error("%s: resending of %d failed; entry not found", local_addr, i);
                break;
            }
            if(entry_term <= 0)
                entry_term=e.term();
            entries.add(e);
        }

        LogEntry prev=log_impl.get(from-1);
        long prev_term=prev != null? prev.term : 0;
        Message msg=new ObjectMessage(target, entries)
          .putHeader(id, new AppendEntriesRequest(this.local_addr, currentTerm(), from - 1, prev_term,
                                                  entry_term, commit_index));
        down_prot.down(msg);
        num_resends++;
    }


    protected void sendSnapshotTo(Address dest) throws Exception {
        LogEntry last_committed_entry=log_impl.get(commitIndex());
        long last_index=commit_index, last_term=last_committed_entry.term;
        takeSnapshot();
        ByteBuffer data=log_impl.getSnapshot();
        log.debug("%s: sending snapshot (%s) to %s", local_addr, Util.printBytes(data.position()), dest);
        Message msg=new BytesMessage(dest, data)
          .putHeader(id, new InstallSnapshotRequest(currentTerm(), leader(), last_index, last_term));
        down_prot.down(msg);
    }

    /**
     * Tries to move commit_index up to index_inclusive, apply the entries in [commit_index+1 .. index_inclusive]
     * to the state machine and notify the clients for each entry. There is no need to check if an entry is committed
     * in RequestTable, as this was done before calling this method.
     * @param index_inclusive The index to which to move commit_index
     * @param serialize_response When true, the response of applying a change to the state machine needs to be serialized
     *                           into a byte[] array, otherwise null can be returned (reducing serialization cost)
     */
    protected RAFT commitLogTo(long index_inclusive, boolean serialize_response) {
        long to=Math.min(last_appended, index_inclusive);
        long last_successful_apply=applyCommits(to, serialize_response);
        commit_index=Math.max(commit_index, last_successful_apply);
        log_impl.commitIndex(commit_index);
        return this;
    }


    /** Appends to the log and returns true if added or false if not (e.g. because the entry already existed */
    protected boolean append(long index, LogEntries entries) {
        if(index <= last_appended)
            return false;
        last_appended=log_impl.append(index, entries);
        snapshotIfNeeded((int)entries.totalSize());
        return true;
    }

    protected void deleteAllLogEntriesStartingFrom(long index) {
        log_impl.deleteAllEntriesStartingFrom(index);
        last_appended=log_impl.lastAppended();
        commit_index=log_impl.commitIndex();
    }

    protected void snapshotIfNeeded(int bytes_added) {
        curr_log_size+=bytes_added;
        if(curr_log_size >= max_log_size) {
            try {
                this.log.debug("%s: current log size is %d, exceeding max_log_size of %d: creating snapshot",
                               local_addr, curr_log_size, max_log_size);
                takeSnapshot();
            }
            catch(Exception ex) {
                log.error("%s: failed snapshotting log: %s", local_addr, ex);
            }
        }
    }

    protected void takeSnapshot() throws Exception {
        if(state_machine == null)
            throw new IllegalStateException("state machine is null");

        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(128, true);
        internal_state.writeTo(out);
        state_machine.writeContentTo(out);
        ByteBuffer buf=ByteBuffer.wrap(out.buffer(), 0, out.position());
        log_impl.setSnapshot(buf);
        log_impl.truncate(commitIndex());
        num_snapshots++;
        // curr_log_size=logSizeInBytes();
        // this is faster than calling logSizeInBytes(), but may not be accurate: if commit-index is way
        // behind last-appended, then this may perform the next truncation later than it should
        curr_log_size=0;
    }

    /**
     * Applies log entries [commit_index+1 .. to_inclusive] to the state machine and notifies clients in RequestTable.
     * @param to_inclusive The end index (inclusive) of the log entries to apply
     * @param serialize_response Whether or not {@link StateMachine#apply(byte[], int, int, boolean)} needs to return a serialized
     *                           response
     * @return The last index of the range of log entries that was successfuly applied (normally this is to_inclusive)
     */
    protected long applyCommits(long to_inclusive, boolean serialize_response) {
        long last_successful_apply=commit_index;
        for(long i=commit_index+1; i <= to_inclusive; i++) {
            try {
                applyCommit(i, serialize_response);
                last_successful_apply=i;
            }
            catch(Throwable t) {
                log.error("%s: failed moving commit_index to %d: %s", local_addr, to_inclusive, t);
                return last_successful_apply;
            }
        }
        return last_successful_apply;
    }

    /** Applies the commit at index */
    protected void applyCommit(long index, boolean serialize_response) throws Exception {
        // Apply the modifications to the state machine
        LogEntry log_entry=log_impl.get(index);
        if(log_entry == null)
            throw new IllegalStateException(local_addr + ": log entry for index " + index + " not found in log");
        byte[] rsp=null;
        RequestTable.Entry<String> entry=request_table != null? request_table.remove(index) : null;
        if(log_entry.internal) {
            try {
                InternalCommand cmd=Util.streamableFromByteBuffer(InternalCommand.class, log_entry.command,
                                                                  log_entry.offset, log_entry.length);
                cmd.execute(this);
            }
            catch(Throwable t) {
                notify(entry, t);
            }
        }
        else {
            Options opts=entry != null? entry.options() : null;
            if(opts != null && opts.ignoreReturnValue())
                serialize_response=false;
            try {
                rsp=state_machine.apply(log_entry.command, log_entry.offset, log_entry.length, serialize_response);
            }
            catch(Throwable t) {
                notify(entry, t);
            }
        }
        notify(entry, rsp);
    }



    public void handleView(View view) {
        boolean check_view=this.view != null && this.view.size() < view.size();
        this.view=view;
        if(commit_table != null) {
            List<Address> mbrs=new ArrayList<>(view.getMembers());
            mbrs.remove(local_addr);
            commit_table.adjust(mbrs, last_appended + 1);
        }

        // if we're the leader, check if the view contains no duplicate raft-ids
        if(check_view && duplicatesInView(view))
            log.error("view contains duplicate raft-ids: %s", view);
    }

    public RAFT setLeaderAndTerm(Address new_leader) {
        return setLeaderAndTerm(new_leader, 0);
    }

    /** Sets the new leader and term */
    public RAFT setLeaderAndTerm(Address new_leader, long new_term) {
        raft_state.tryAdvanceTermAndLeader(new_term, new_leader);
        return this;
    }

    public int trySetLeaderAndTerm(Address newLeader, long newTerm) {
        return raft_state.tryAdvanceTermAndLeader(newTerm, newLeader);
    }

    private void leaderUpdated(Address new_leader) {
        if(Objects.equals(local_addr, new_leader)) {
            if(!isLeader())
                log.debug("%s: becoming Leader", local_addr);
            changeRole(Role.Leader);   // no-op if already a leader
        }
        else
            changeRole(Role.Follower); // no-op if already a follower
    }

    protected static <T> void notify(RequestTable.Entry<T> e, byte[] rsp) {
        if(e != null)
            e.notify(rsp);
    }

    protected static <T> void notify(RequestTable.Entry<T> e, Throwable t) {
        if(e != null)
            e.notify(t);
    }

    protected RAFT changeRole(Role new_role) {
        RaftImpl new_impl=new_role == Role.Leader? new Leader(this) : new Follower(this);
        RaftImpl old_impl=impl;
        if(old_impl == null || !old_impl.getClass().equals(new_impl.getClass())) {
            if(old_impl != null)
                old_impl.destroy();
            new_impl.init();
            impl=new_impl;
            log.trace("%s: changed role from %s -> %s", local_addr, old_impl == null? "null" :
              old_impl.getClass().getSimpleName(), new_impl.getClass().getSimpleName());
            notifyRoleChangeListeners(new_role);
        }
        return this;
    }

    /** If cmd is not null, execute it. Else parse buf into InternalCommand then call cmd.execute() */
    protected void executeInternalCommand(InternalCommand cmd, byte[] buf, int offset, int length) {
        if(cmd == null) {
            try {
                cmd=Util.streamableFromByteBuffer(InternalCommand.class, buf, offset, length);
            }
            catch(Exception ex) {
                log.error("%s: failed unmarshalling internal command: %s", local_addr, ex);
                return;
            }
        }

        try {
            cmd.execute(this);
        }
        catch(Exception ex) {
            log.error("%s: failed executing internal command %s: %s", local_addr, cmd, ex);
        }
    }

    protected String createLogName(String name, String suffix) {
        if(!suffix.startsWith("."))
            suffix="." + suffix;
        boolean needs_suffix=!name.endsWith(suffix);
        String retval=name;
        if(!new File(name).isAbsolute()) {
            retval=log_dir + File.separator + name;
        }
        return needs_suffix? retval + suffix : retval;
    }

    protected void notifyRoleChangeListeners(Role role) {
        for(RoleChange ch: role_change_listeners) {
            try {
                ch.roleChanged(role);
            }
            catch(Throwable ignored) {}
        }
    }

    /** Checks if a given view contains duplicate raft-ids. Uses key raft-id in ExtendedUUID to compare */
    protected boolean duplicatesInView(View view) {
        Set<String> mbrs=new HashSet<>();
        for(Address addr : view) {
            if(!(addr instanceof ExtendedUUID))
                log.warn("address %s is not an ExtendedUUID but a %s", addr, addr.getClass().getSimpleName());
            else {
                ExtendedUUID uuid=(ExtendedUUID)addr;
                byte[] val=uuid.get(raft_id_key);
                String m=val != null? Util.bytesToString(val) : null;
                if(m == null)
                    log.error("address %s doesn't have a raft-id", addr);
                else if(!mbrs.add(m))
                    return true;
            }
        }
        return false;
    }

    protected static Map<String,String> parseCommaDelimitedProps(String s) {
        if (s == null)
            return null;
        Map<String,String> props=new HashMap<>();
        Pattern p=Pattern.compile("\\s*([^=\\s]+)\\s*=\\s*([^=\\s,]+)\\s*,?"); //Pattern.compile("\\s*([^=\\s]+)\\s*=\\s([^=\\s]+)\\s*,?");
        Matcher matcher=p.matcher(s);
        while(matcher.find()) {
            props.put(matcher.group(1), matcher.group(2));
        }
        return props;
    }


    public interface RoleChange {
        void roleChanged(Role role);
    }

    protected void computeMajority() {
        majority=(internal_state.getMembers().size() / 2) + 1;
    }


    protected static class Request {

        protected void failed(Throwable t) { }

    }

    /** Received by up(Message) or up(MessageBatch) */
    protected static class UpRequest extends Request {
        private final Message    msg;
        private final RaftHeader hdr;

        public UpRequest(Message msg, RaftHeader hdr) {
            this.msg=msg;
            this.hdr=hdr;
        }

        public String toString() {
            return String.format("%s %s", UpRequest.class.getSimpleName(), hdr);
        }
    }

    /** Generated by {@link RAFT#setAsync(byte[], int, int)} */
    protected static class DownRequest extends Request {
        final CompletableFuture<byte[]> f;
        final byte[]                    buf;
        final int                       offset, length;
        final boolean                   internal;
        final Options                   options;

        public DownRequest(CompletableFuture<byte[]> f, byte[] buf, int offset, int length,
                           boolean internal, Options opts) {
            this.f=f;
            this.buf=buf;
            this.offset=offset;
            this.length=length;
            this.internal=internal;
            this.options=opts;
        }

        @Override
        protected final void failed(Throwable t) {
            f.completeExceptionally(t);
        }

        public String toString() {
            return String.format("%s %d bytes", DownRequest.class.getSimpleName(), length);
        }
    }

    protected static class SnapshotRequest extends Request {
        final CompletableFuture<Void> f;

        public SnapshotRequest(CompletableFuture<Void> f) {
            this.f = f;
        }

        @Override
        protected final void failed(Throwable t) {
            f.completeExceptionally(t);
        }

        @Override
        public String toString() {
            return SnapshotRequest.class.getSimpleName();
        }
    }
}
