package org.jgroups.raft;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;

import java.util.concurrent.CompletableFuture;
import java.util.function.ObjLongConsumer;

/**
 * Main interaction point for applications with jgroups-raft.
 * <p>
 * Provides methods to make changes, register a state machine,
 * get commit-index and last-applied, register {@link org.jgroups.protocols.raft.RAFT.RoleChange} listeners etc.
 * </p>
 * Sample use:
 * <pre>
 *     JChannel ch=createChannel();
 *     RaftHandle handle=new RaftHandle(ch, new StateMachineImpl()); // implements StateMachine
 *     handle.addRoleListener(this);
 *     handle.setAsync(buf, 0, buf.length).whenComplete((buf,ex) -> ...);
 * </pre>
 * @author Bela Ban
 * @since  0.2
 */
public class RaftHandle implements Settable {
    protected JChannel     ch;
    protected RAFT         raft;
    protected Settable     settable; // usually REDIRECT (at the top of the stack)

    /**
     * Creates a RaftHandle instance.
     * @param ch The channel over which to create the RaftHandle. Must be non-null, but doesn't yet need to be connected
     * @param sm An implementation of {@link StateMachine}.
     *           Can be null, ie. if it is set later via {@link #stateMachine(StateMachine)}.
     */
    public RaftHandle(JChannel ch, StateMachine sm) {
        if((this.ch=ch) == null)
            throw new IllegalStateException("channel must not be null");
        if((raft=RAFT.findProtocol(RAFT.class, ch.getProtocolStack().getTopProtocol(),true)) == null)
            throw new IllegalStateException("RAFT protocol was not found");
        if((settable=RAFT.findProtocol(Settable.class, ch.getProtocolStack().getTopProtocol(),true)) == null)
            throw new IllegalStateException("did not find a protocol implementing Settable (e.g. REDIRECT or RAFT)");
        stateMachine(sm);
    }

    public JChannel     channel()                                    {return ch;}
    public RAFT         raft()                                       {return raft;}
    public String       raftId()                                     {return raft.raftId();}
    public RaftHandle   raftId(String id)                            {raft.raftId(id); return this;}
    public Address      leader()                                     {return raft.leader();}
    public boolean      isLeader()                                   {return raft.isLeader();}
    public StateMachine stateMachine()                               {return raft.stateMachine();}
    public RaftHandle   stateMachine(StateMachine sm)                {raft.stateMachine(sm); return this;}
    public RaftHandle   addRoleListener(RAFT.RoleChange listener)    {raft.addRoleListener(listener); return this;}
    public RaftHandle   removeRoleListener(RAFT.RoleChange listener) {raft.remRoleListener(listener); return this;}
    public long         currentTerm()                                {return raft.currentTerm();}
    public long         lastApplied()                                {return raft.lastAppended();}
    public long         commitIndex()                                {return raft.commitIndex();}
    public void         snapshot() throws Exception                  {raft.snapshot();}
    public Log          log()                                        {return raft.log();}
    public long         logSize()                                    {return raft.logSize();}

    /**
     * Asynchronously adds a new server to the RAFT cluster.
     *
     * <p>Only a single membership change executes at any given time. Multiple concurrent changes are chained
     * and executed serially. The membership change adds an entry to the log, meaning it tolerates crashes and
     * restarts and only has an effect after a majority of members commit it.</p>
     *
     * @param server The server RAFT ID to add.
     * @return A {@link CompletableFuture} that completes once the command is committed.
     * @throws Exception if dynamic view changes are not enabled.
     * @throws IllegalStateException if the node is not the leader.
     */
    public CompletableFuture<byte[]> addServer(String server) throws Exception {
        return raft.addServer(server);
    }

    /**
     * Asynchronously removes a server from the RAFT cluster.
     *
     * @param server The server RAFT ID to remove.
     * @return A {@link CompletableFuture} that completes once the command is committed.
     * @throws Exception if dynamic view changes are not enabled.
     * @throws IllegalStateException if the node is not the leader.
     * @see #addServer(String)
     */
    public CompletableFuture<byte[]> removeServer(String server) throws Exception {
        return raft.removeServer(server);
    }

    public void logEntries(ObjLongConsumer<LogEntry> func) {
        raft.logEntries(func);
    }

    @Override
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Options options) throws Exception {
        return settable.setAsync(buf, offset, length, options);
    }

    @Override
    public CompletableFuture<byte[]> getAsync(byte[] buf, int offset, int length, Options options) throws Exception {
        return settable.getAsync(buf, offset, length, options);
    }
}
