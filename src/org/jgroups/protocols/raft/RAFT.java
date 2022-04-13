package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.LogCache;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Implementation of the <a href="https://github.com/ongardie/dissertation">RAFT consensus protocol</a> in JGroups<p/>
 * [1] https://github.com/ongardie/dissertation<br/>
 * The implementation uses a queue to which the following types of requests are added: down-requests (invocations of
 * {@link #setAsync(byte[], int, int, InternalCommand)})
 * and up-requests (requests or responses received in {@link #up(Message)} or {@link #up(MessageBatch)}).<br/>
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
    }

    @Property(description="The identifier of this node. Needs to be unique and an element of members. Must not be null",
              writable=false)
    protected String                  raft_id;

    protected final List<String>      members=new ArrayList<>();

    @ManagedAttribute(description="Majority needed to achieve consensus; computed from members)")
    protected int                     majority=-1;

    @Property(description="If true, we can change 'members' at runtime")
    protected boolean                 dynamic_view_changes=true;

    @Property(description="The fully qualified name of the class implementing Log")
    protected String                  log_class="org.jgroups.protocols.raft.LevelDBLog";

    @Property(description="Arguments to the log impl, e.g. k1=v1,k2=v2. These will be passed to init()")
    protected String                  log_args;

    @Property(description="The directory in which the log and snapshots are stored. Defaults to the temp dir")
    protected String                  log_dir=Util.checkForMac()?
      File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");

    @Property(description="The prefix of the log and snapshot. If null, the logical name of the channel is used as prefix")
    protected String                  log_prefix;

    @ManagedAttribute(description="The name of the log")
    protected String                  log_name;

    @ManagedAttribute(description="The name of the snapshot")
    protected String                  snapshot_name;

    @Property(description="Interval (ms) at which AppendEntries messages are resent to members with missing log entries",
      type=AttributeType.TIME)
    protected long                    resend_interval=1000;

    @Property(description="Send commit message to followers immediately after leader commits (majority has consensus). " +
      "Caution : it may generate more traffic than expected")
    protected boolean                 send_commits_immediately;

    @Property(description="Max number of bytes a log can have until a snapshot is created",type=AttributeType.BYTES)
    protected int                     max_log_size=1_000_000;

    protected int                     _max_log_cache_size=1024;

    @ManagedAttribute(description="The current size of the log in bytes",type=AttributeType.BYTES)
    protected int                     curr_log_size; // keeps counts of the bytes added to the log

    @ManagedAttribute(description="Number of successful AppendEntriesRequests")
    protected int                     num_successful_append_requests;

    @ManagedAttribute(description="Number of failed AppendEntriesRequests because the entry wasn't found in the log")
    protected int                     num_failed_append_requests_not_found;

    @ManagedAttribute(description="Number of failed AppendEntriesRequests because the prev entry's term didn't match")
    protected int                     num_failed_append_requests_wrong_term;

    protected StateMachine            state_machine;

    protected boolean                 state_machine_loaded;

    protected Log                     log_impl;

    protected RequestTable<String>    request_table;
    protected CommitTable             commit_table;

    protected final List<RoleChange>  role_change_listeners=new ArrayList<>();

    // Set to true during an addServer()/removeServer() op until the change has been committed
    protected final AtomicBoolean     members_being_changed = new AtomicBoolean(false);

    /** The current role (follower, candidate or leader). Every node starts out as a follower */
    protected volatile RaftImpl       impl=new Follower(this);
    protected volatile View           view;

    /** The current leader (can be null if there is currently no leader) */
    @ManagedAttribute(description="The current leader")
    protected volatile Address        leader;

    /** The current term. Incremented when this node becomes a candidate, or set when a higher term is seen */
    @ManagedAttribute(description="The current term")
    protected int                     current_term;

    @ManagedAttribute(description="Index of the highest log entry appended to the log",type=AttributeType.SCALAR)
    protected int                     last_appended;

    @ManagedAttribute(description="Index of the last committed log entry",type=AttributeType.SCALAR)
    protected int                     commit_index;

    @ManagedAttribute(description="The number of snapshots performed")
    protected int                     num_snapshots;

    @ManagedAttribute(description="The number of times AppendEntriesRequests were resent")
    protected int                     num_resends;

    protected boolean                 snapshotting;

    @Property(description="Max size in items the processing queue can have",type=AttributeType.SCALAR)
    protected int                     processing_queue_max_size=9182;

    /** All requests are added to this queue; a single thread processes this queue - hence no synchronization issues */
    protected BlockingQueue<Request>  processing_queue;

    protected final List<Request>     remove_queue=new ArrayList<>();

    protected Runner                  runner; // the single thread processing the request queue

    protected boolean                 synchronous; // used by the synchronous execution framework (only for testing)


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
    public String       snapshotName()                {return snapshot_name;}
    public long         resendInterval()              {return resend_interval;}
    public RAFT         resendInterval(long val)      {resend_interval=val; return this;}
    public boolean      sendCommitsImmediately()      {return send_commits_immediately;}
    public RAFT         sendCommitsImmediately(boolean v) {send_commits_immediately=v; return this;}
    public int          maxLogSize()                  {return max_log_size;}
    public RAFT         maxLogSize(int val)           {max_log_size=val; return this;}
    public int          currentLogSize()              {return curr_log_size;}
    public int          requestTableSize()            {return request_table.size();}
    public int          numSnapshots()                {return num_snapshots;}
    public Address      leader()                      {return leader;}
    public RAFT         leader(Address new_leader)    {this.leader=new_leader; return this;}
    public boolean      isLeader()                    {return Objects.equals(leader, local_addr);}
    public org.jgroups.logging.Log getLog()           {return this.log;}
    public RAFT         stateMachine(StateMachine sm) {this.state_machine=sm; return this;}
    public StateMachine stateMachine()                {return state_machine;}
    public CommitTable  commitTable()                 {return commit_table;}
    public int          currentTerm()                 {return current_term;}
    public int          lastAppended()                {return last_appended;}
    public int          commitIndex()                 {return commit_index;}
    public Log          log()                         {return log_impl;}
    public RAFT         log(Log new_log)              {this.log_impl=new_log; return this;}
    public RAFT         addRoleListener(RoleChange c) {this.role_change_listeners.add(c); return this;}
    public RAFT         remRoleListener(RoleChange c) {this.role_change_listeners.remove(c); return this;}
    public RAFT         stateMachineLoaded(boolean b) {this.state_machine_loaded=b; return this;}
    public boolean      synchronous()                 {return synchronous;}
    public RAFT         synchronous(boolean b)        {synchronous=b; return this;}


    public void resetStats() {
        super.resetStats();
        num_snapshots=num_resends=num_successful_append_requests=num_failed_append_requests_not_found
          =num_failed_append_requests_wrong_term=0;
        if(log_impl instanceof LogCache)
            ((LogCache)log_impl).resetStats();
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
        this.members.clear();
        this.members.addAll(new HashSet<>(list));
        computeMajority();
        return this;
    }

    @Property public List<String> members() {
        return new ArrayList<>(members);
    }


    /**
     * Sets current_term if new_term is bigger
     * @param new_term The new term
     * @return -1 if new_term is smaller, 0 if equal and 1 if new_term is bigger
     */
    public synchronized int currentTerm(final int new_term)  {
        if(new_term < current_term)
            return -1;
        if(new_term > current_term) {
            log.trace("%s: changed term from %d -> %d", local_addr, current_term, new_term);
            current_term=new_term;
            log_impl.currentTerm(new_term);
            return 1;
        }
        return 0;
    }

    @ManagedAttribute(description="The current role")
    public String role()            {return impl.getClass().getSimpleName();}

    @ManagedOperation(description="Dumps the commit table")
    public String dumpCommitTable() {return commit_table != null? commit_table.toString() : "n/a";}

    @ManagedAttribute(description="Number of log entries in the log")
    public int logSize()            {return log_impl.size();}

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
    public int logSizeInBytes() {
        final AtomicInteger count=new AtomicInteger(0);
        log_impl.forEach((entry,index) -> count.addAndGet(entry.length()));
        return count.intValue();
    }

    @ManagedOperation(description="Dumps the last N log entries")
    public String dumpLog(int last_n) {
        final StringBuilder sb=new StringBuilder();
        int to=last_appended, from=Math.max(1, to-last_n);
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

    public RAFT deleteSnapshot() {
        File file=new File(snapshot_name);
        file.delete();
        return this;
    }

    public RAFT deleteLog() throws Exception {
        if(log_impl != null) {
            log_impl.delete();
            log_impl=null;
        }
        return this;
    }

    public void logEntries(ObjIntConsumer<LogEntry> func) {
        log_impl.forEach(func);
    }

    public synchronized int createNewTerm() {
        return ++current_term;
    }

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
    public synchronized void snapshot() throws Exception {
        if(snapshotting) {
            log.error("%s: cannot create snapshot; snapshot is being created by another thread");
            return;
        }
        try {
            snapshotting=true;
            doSnapshot();
            num_snapshots++;
        }
        finally {
            snapshotting=false;
        }
    }

    /** Loads the log entries from [first .. commit_index] into the state machine */
    @ManagedOperation(description="Reads the snapshot (if present) and loads log entries from [first .. commit_index] " +
      "into the state machine")
    public void initStateMachineFromLog() throws Exception {
        if(state_machine == null || state_machine_loaded)
            return;
        int snapshot_offset=0;  // 0 when no snapshot is present, 1 otherwise
        try(InputStream input=new FileInputStream(snapshot_name)) {
            state_machine.readContentFrom(new DataInputStream(input));
            snapshot_offset=1;
            log.debug("%s: initialized state machine from snapshot %s", local_addr, snapshot_name);
        }
        catch(FileNotFoundException fne) {
        }

        int from=Math.max(1, log_impl.firstAppended()+snapshot_offset), to=commit_index, count=0;
        for(int i=from; i <= to; i++) {
            LogEntry log_entry=log_impl.get(i);
            if(log_entry == null) {
                log.error("%s: log entry for index %d not found in log", local_addr, i);
                break;
            }
            if(log_entry.command != null) {
                if(log_entry.internal)
                    executeInternalCommand(null, log_entry.command, log_entry.offset, log_entry.length);
                else {
                    state_machine.apply(log_entry.command, log_entry.offset, log_entry.length);
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
        Set<String> tmp=new HashSet<>(members);
        if(tmp.size() != members.size()) {
            log.error("members (%s) contains duplicates; removing them and setting members to %s", members, tmp);
            members.clear();
            members.addAll(tmp);
        }
        computeMajority();

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

        if(raft_id == null)
            raft_id=InetAddress.getLocalHost().getHostName();

        if(!members.contains(raft_id))
            throw new IllegalStateException(String.format("raft-id %s is not listed in members %s", raft_id, members));

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
            snapshot_name=log_prefix;
            log_name=createLogName(log_prefix, "log");
            snapshot_name=createLogName(snapshot_name, "snapshot");
            log_impl.init(log_name, args);
        }

        if(!(local_addr instanceof ExtendedUUID))
            throw new IllegalStateException("local address must be an ExtendedUUID but is a " + local_addr.getClass().getSimpleName());

        last_appended=log_impl.lastAppended();
        commit_index=log_impl.commitIndex();
        current_term=log_impl.currentTerm();
        log.trace("set last_appended=%d, commit_index=%d, current_term=%d", last_appended, commit_index, current_term);
        if(snapshot_name != null)
            initStateMachineFromLog();
        curr_log_size=logSizeInBytes();

        if(_max_log_cache_size > 0)  // the log cache is enabled
            log_impl=new LogCache(log_impl, _max_log_cache_size);
        runner.start();
    }


    @Override public void stop() {
        super.stop();
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

    public void flushCommitTable(Address member) {
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
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length) {
        return setAsync(buf, offset, length, null);
    }

    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, InternalCommand cmd) {
        if(leader == null || (local_addr != null && !leader.equals(local_addr)))
            throw new IllegalStateException("I'm not the leader (local_addr=" + local_addr + ", leader=" + leader + ")");
        if(buf == null)
            throw new IllegalArgumentException("buffer must not be null");
        CompletableFuture<byte[]> retval=new CompletableFuture<>();
        RequestTable<String> reqtab=request_table;
        if(reqtab == null) {
            retval.completeExceptionally(new IllegalStateException("request table was null on " + impl.getClass().getSimpleName()));
            return retval;
        }
        if(synchronous)
            handleDownRequest(retval, buf, offset, length, cmd);
        else
            add(new DownRequest(retval, buf, offset, length, cmd)); // will call handleDownRequest()
        return retval; // 4. Return CompletableFuture
    }

    public String toString() {
        return String.format("%s %s: commit=%d last-appended=%d curr-term=%d",
                             RAFT.class.getSimpleName(), local_addr, commit_index, last_appended, current_term);
    }

    protected void add(Request r) {
        try {
            processing_queue.put(r);
        }
        catch(InterruptedException ex) {
            log.error("%s: failed adding %s to processing queue: %s", local_addr, r, ex);
        }
    }

    /** This method is always called by a single thread only, and does therefore not need to be reentrant */
    protected void handleDownRequest(CompletableFuture<byte[]> retval, byte[] buf, int offset, int length,
                                     InternalCommand cmd) {
        if(leader == null || !Objects.equals(leader,local_addr))
            throw new IllegalStateException("I'm not the leader (local_addr=" + local_addr + ", leader=" + leader + ")");

        RequestTable<String> reqtab=request_table;

        // 1. Append to the log
        int prev_index=last_appended;
        int curr_index=++last_appended;
        LogEntry entry=log_impl.get(prev_index);
        int prev_term=entry != null? entry.term : 0;

        log_impl.append(curr_index, true, new LogEntry(current_term, buf, offset, length, cmd != null));
        num_successful_append_requests++;

        if(cmd != null)
            executeInternalCommand(cmd, null, 0, 0);

        // 2. Add the request to the client table, so we can return results to clients when done
        reqtab.create(curr_index, raft_id, retval, majority());

        // 3. Multicast an AppendEntries message (exclude self)
        Message msg=new BytesMessage(null, buf, offset, length)
          .putHeader(id, new AppendEntriesRequest(this.local_addr, current_term, prev_index, prev_term,
                                                  current_term, commit_index, cmd != null))
          .setFlag(Message.TransientFlag.DONT_LOOPBACK); // don't receive my own request
        down_prot.down(msg);

        snapshotIfNeeded(length);
        if(reqtab.isCommitted(curr_index))
            handleCommit(curr_index);
    }

    public void handleUpRequest(Message msg, RaftHeader hdr) {
        // if hdr.term < current_term -> drop message
        // if hdr.term > current_term -> set current_term and become Follower, accept message
        // if hdr.term == current_term -> accept message
        if(currentTerm(hdr.curr_term) < 0)
            return;

        RaftImpl ri=impl;
        if(ri == null)
            return;

        if(hdr instanceof AppendEntriesRequest) {
            AppendEntriesRequest r=(AppendEntriesRequest)hdr;
            AppendResult res=ri.handleAppendEntriesRequest(msg.getArray(), msg.getOffset(), msg.getLength(), msg.src(),
                                                           r.prev_log_index, r.prev_log_term, r.entry_term,
                                                           r.leader_commit, r.internal);
            res.commitIndex(commit_index);
            Message rsp=new EmptyMessage(msg.src()).putHeader(id, new AppendEntriesResponse(current_term, res));
            down_prot.down(rsp);
        }
        else if(hdr instanceof AppendEntriesResponse) {
            AppendEntriesResponse rsp=(AppendEntriesResponse)hdr;
            ri.handleAppendEntriesResponse(msg.src(),rsp.curr_term, rsp.result);
        }
        else if(hdr instanceof InstallSnapshotRequest) {
            InstallSnapshotRequest req=(InstallSnapshotRequest)hdr;
            ri.handleInstallSnapshotRequest(msg, req.curr_term, req.leader, req.last_included_index, req.last_included_term);
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
                if(remove_queue.isEmpty())
                    return;
                else
                    process(remove_queue);
            }
        }
        catch(InterruptedException ignored) {
        }
    }

    protected void process (List<Request> q) {
        for(Request r: q) {
            try {
                if(r instanceof UpRequest) {
                    UpRequest up=(UpRequest)r;
                    handleUpRequest(up.msg, up.hdr);
                }
                else if(r instanceof DownRequest) {
                    DownRequest dr=(DownRequest)r;
                    handleDownRequest(dr.f, dr.buf, dr.offset, dr.length, dr.cmd);
                }
            }
            catch(Throwable ex) {
                log.error("%s: failed handling request %s: %s", local_addr, r, ex);
            }
        }
    }

    /** Populate with non-committed entries (from log) (https://github.com/belaban/jgroups-raft/issues/31) */
    protected void createRequestTable() {
        request_table=new RequestTable<>();
        for(int i=this.commit_index+1; i <= this.last_appended; i++)
            request_table.create(i, raft_id, null, majority());
    }

    protected void createCommitTable() {
        List<Address> jg_mbrs=view != null? view.getMembers() : new ArrayList<>();
        List<Address> mbrs=new ArrayList<>(jg_mbrs);
        mbrs.remove(local_addr);
        commit_table=new CommitTable(mbrs, last_appended +1);
    }

    protected void _addServer(String name) {
        if(name == null) return;
        if(!members.contains(name)) {
            members.add(name);
            computeMajority();
        }
    }

    protected void _removeServer(String name) {
        if(name == null) return;
        if(members.remove(name))
            computeMajority();
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
            if(e.snapshotInProgress(true)) {
                try {
                    sendSnapshotTo(member); // will reset snapshot_in_progress
                }
                catch(Exception ex) {
                    log.error("%s: failed sending snapshot to %s: next_index=%d, first_applied=%d",
                              local_addr, member, e.nextIndex(), log().firstAppended());
                }
            }
            return;
        }
        if(this.last_appended >= e.nextIndex()) {
            int to=e.sendSingleMessage()? e.nextIndex() : last_appended;
            for(int i=Math.max(e.nextIndex(),1); i <= to; i++) {  // i=match_index+1 ?
                if(log.isTraceEnabled())
                    log.trace("%s: resending %d to %s", local_addr, i, member);
                resend(member, i);
            }
            return;
        }
        if(this.last_appended > e.matchIndex()) {
            int index=this.last_appended;
            if(index > 0) {
                log.trace("%s: resending %d to %s", local_addr, index, member);
                resend(member, index);
            }
            return;
        }
        if(this.commit_index > e.commitIndex()) { // send an empty AppendEntries message as commit message
            Message msg=new EmptyMessage(member)
              .putHeader(id, new AppendEntriesRequest(this.local_addr, current_term, 0, 0,
                                                      current_term, this.commit_index, false));
            down_prot.down(msg);
            return;
        }
        if(this.commit_index < this.last_appended) { // fixes https://github.com/belaban/jgroups-raft/issues/30
            for(int i=this.commit_index+1; i <= this.last_appended; i++)
                resend(member, i);
        }
    }

    protected CompletableFuture<byte[]> changeMembers(String name, InternalCommand.Type type) throws Exception {
        if(!dynamic_view_changes)
            throw new Exception("dynamic view changes are not allowed; set dynamic_view_changes to true to enable it");
        if(leader == null || !Objects.equals(leader, local_addr))
            throw new IllegalStateException("I'm not the leader (local_addr=" + local_addr + ", leader=" + leader + ")");

        if(members_being_changed.compareAndSet(false, true)) {
            InternalCommand cmd=new InternalCommand(type, name);
            byte[] buf=Util.streamableToByteBuffer(cmd);
            return setAsync(buf, 0, buf.length, cmd);
        }
        else
            throw new IllegalStateException(String.format("%s(%s) cannot be invoked as previous operation has not yet been committed",
                                                          type, name));
    }

    protected void resend(Address target, int index) {
        LogEntry entry=log_impl.get(index);
        if(entry == null) {
            log.error("%s: resending of %d failed; entry not found", local_addr, index);
            return;
        }
        LogEntry prev=log_impl.get(index-1);
        int prev_term=prev != null? prev.term : 0;
        Message msg=new BytesMessage(target).setArray(entry.command, entry.offset, entry.length)
          .putHeader(id, new AppendEntriesRequest(this.local_addr, current_term, index - 1, prev_term,
                                                  entry.term, commit_index, entry.internal));
        down_prot.down(msg);
        num_resends++;
    }

    protected void doSnapshot() throws Exception {
        if(state_machine == null)
            throw new IllegalStateException("state machine is null");
        try (OutputStream output=new FileOutputStream(snapshot_name)) {
            state_machine.writeContentTo(new DataOutputStream(output));
        }
        log_impl.truncate(commitIndex());
    }

    protected boolean snapshotExists() {
        File file=new File(snapshot_name);
        return file.exists();
    }

    protected void sendSnapshotTo(Address dest) throws Exception {
        try {
            if(snapshotting)
                return;
            snapshotting=true;

            LogEntry last_committed_entry=log_impl.get(commitIndex());
            int last_index=commit_index, last_term=last_committed_entry.term;
            doSnapshot();

            byte[] data=Files.readAllBytes(Paths.get(snapshot_name));
            log.debug("%s: sending snapshot (%s) to %s", local_addr, Util.printBytes(data.length), dest);
            Message msg=new BytesMessage(dest, data)
              .putHeader(id, new InstallSnapshotRequest(currentTerm(), leader(), last_index, last_term));
            down_prot.down(msg);
        }
        finally {
            snapshotting=false;
            if(commit_table != null)
                commit_table.snapshotInProgress(dest, false);
        }
    }

    /**
     * Received a majority of votes for the entry at index. Note that indices may be received out of order, e.g. if
     * we have modifications at indices 4, 5 and 6, entry[5] might get a majority of votes (=committed)
     * before entry[3] and entry[6].<p/>
     * The following things are done:
     * <ul>
     *     <li>See if commit_index can be moved to index (incr commit_index until a non-committed entry is encountered)</li>
     *     <li>For each committed entry, apply the modification at entry[index] to the state machine</li>
     *     <li>For each committed entry, notify the client and set the result (CompletableFuture)</li>
     * </ul>
     * @param index The index of the committed entry.
     */
    protected void handleCommit(int index) {
        try {
            for(int i=commit_index + 1; i <= Math.min(index, last_appended); i++) {
                if(!request_table.isCommitted(i))
                    break; // stop at the first uncommitted request
                applyCommit(i);
                commit_index=Math.max(commit_index, i);
            }
        }
        catch(Throwable t) {
            log.error("failed applying commit %d: %s", index, t);
        }
    }

    /**
     * Tries to advance commit_index up to leader_commit, applying all uncommitted log entries to the state machine
     * @param leader_commit The commit index of the leader
     */
    protected RAFT commitLogTo(int leader_commit) {
        int old_commit=commit_index, to=Math.min(last_appended, leader_commit);
        try {
            for(int i=commit_index+1; i <= to; i++) {
                applyCommit(i);
                commit_index=Math.max(commit_index, i);
            }
        }
        catch(Throwable t) {
            log.error("%s: failed moving commit_index from (exclusive) %d to (inclusive) %d " +
                        "(last_appended=%d, leader's commit_index=%d, failed at commit_index %d)): %s",
                      local_addr, old_commit, to, last_appended, leader_commit, commit_index+1,  t);
        }
        return this;
    }

    /** Appends to the log and returns true if added or false if not (e.g. because the entry already existed */
    protected boolean append(int term, int index, byte[] data, int offset, int length, boolean internal) {
        if(index <= last_appended)
            return false;
        LogEntry entry=new LogEntry(term, data, offset, length, internal);
        log_impl.append(index, true, entry);
        last_appended=log_impl.lastAppended();
        snapshotIfNeeded(length);
        return true;
    }

    protected void deleteAllLogEntriesStartingFrom(int index) {
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
                snapshot();
                curr_log_size=logSizeInBytes();
            }
            catch(Exception ex) {
                log.error("%s: failed snapshotting log: %s", local_addr, ex);
            }
        }
    }

    /** Applies the commit at index */
    protected void applyCommit(int index) throws Exception {
        // Apply the modifications to the state machine
        LogEntry log_entry=log_impl.get(index);
        if(log_entry == null)
            throw new IllegalStateException(local_addr + ": log entry for index " + index + " not found in log");
        if(state_machine == null)
            throw new IllegalStateException(local_addr + ": state machine is null");
        byte[] rsp=null;
        if(log_entry.internal) {
            InternalCommand cmd;
            try {
                cmd=Util.streamableFromByteBuffer(InternalCommand.class, log_entry.command,
                                                  log_entry.offset, log_entry.length);
                if(cmd.type() == InternalCommand.Type.addServer || cmd.type() == InternalCommand.Type.removeServer)
                    members_being_changed.set(false); // new addServer()/removeServer() operations can now be started
            }
            catch(Throwable t) {
                log.error("%s: failed unmarshalling internal command: %s", local_addr, t);
            }
        }
        else
            rsp=state_machine.apply(log_entry.command, log_entry.offset, log_entry.length);

        log_impl.commitIndex(index);

        // Notify the client's CompletableFuture and then remove the entry in the client request table
        if(request_table != null)
            request_table.notifyAndRemove(index, rsp);
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
    public RAFT setLeaderAndTerm(Address new_leader, int new_term) {
        if(Objects.equals(local_addr, new_leader)) {
            if(!isLeader())
                log.debug("%s: becoming Leader", local_addr);
            changeRole(Role.Leader);   // no-op if already a leader
        }
        else
            changeRole(Role.Follower); // no-op if already a follower
        if(new_term > 0)
            currentTerm(new_term);
        return leader(new_leader);
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
        majority=(members.size() / 2) + 1;
    }


    protected static class Request {

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

    /** Generated by {@link org.jgroups.protocols.raft.RAFT#setAsync(byte[], int, int)} */
    protected static class DownRequest extends Request {
        final CompletableFuture<byte[]> f;
        final byte[]                    buf;
        final int                       offset, length;
        final InternalCommand           cmd;

        public DownRequest(CompletableFuture<byte[]> f, byte[] buf, int offset, int length, InternalCommand cmd) {
            this.f=f;
            this.buf=buf;
            this.offset=offset;
            this.length=length;
            this.cmd=cmd;
        }

        public String toString() {
            return String.format("%s %d bytes", DownRequest.class.getSimpleName(), length);
        }
    }
}
