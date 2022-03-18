package org.jgroups.raft.testfwk;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

/**
 * Orchestrates a number of {@link RaftNode} objects, to be used in a unit test, for example a leader and a follower.
 * @author Bela Ban
 * @since  1.0.5
 */
public class RaftCluster {
    // used to 'send' requests between the various instances
    protected final Map<Address,RaftNode> nodes=new ConcurrentHashMap<>();
    protected final Map<Address,RaftNode> dropped_members=new ConcurrentHashMap<>();
    protected final Executor              thread_pool=createThreadPool(1000);
    protected boolean                     async;

    public RaftCluster add(Address addr, RaftNode node) {
        nodes.put(addr, node);
        return this;
    }

    public boolean     async()                          {return async;}
    public RaftCluster async(boolean b)                 {async=b; return this;}
    public RaftCluster remove(Address addr)             {nodes.remove(addr); return this;}
    public RaftCluster clear()                          {nodes.clear(); return this;}
    public boolean     dropTraffic()                    {return !dropped_members.isEmpty();}
    public RaftCluster dropTrafficTo(Address a)         {move(a, nodes, dropped_members); return this;}
    public RaftCluster clearDroppedTrafficTo(Address a) {move(a, dropped_members, nodes); return this;}
    public RaftCluster clearDroppedTraffic()            {moveAll(dropped_members, nodes); return this;}

    public void handleView(View view) {
        List<Address> members=view.getMembers();
        nodes.keySet().retainAll(Objects.requireNonNull(members));
        nodes.values().forEach(n -> n.handleView(view));
    }

    public void send(Message msg) {
        send(msg, false);
    }

    public void send(Message msg, boolean async) {
        Address dest=msg.dest(), src=msg.src();
        if(dest != null) {
            RaftNode node=nodes.get(dest);
            if(this.async || async)
                deliverAsync(node, msg);
            else
                node.up(msg);
        }
        else {
            for(Map.Entry<Address,RaftNode> e: nodes.entrySet()) {
                Address d=e.getKey();
                RaftNode n=e.getValue();
                if(Objects.equals(d, src) && msg.isFlagSet(DONT_LOOPBACK))
                    continue;
                if(this.async || async)
                    deliverAsync(n, msg);
                else
                    n.up(msg);
            }
        }
    }

    protected void deliverAsync(RaftNode n, Message msg) {
        thread_pool.execute(() -> n.up(msg));
    }

    public String toString() {
        return String.format("%d nodes: %s%s", nodes.size(), nodes.keySet(),
                             dropTraffic()? String.format(" (dropping traffic to %s)", dropped_members.keySet()) : "");
    }

    protected static void move(Address key, Map<Address,RaftNode> from, Map<Address,RaftNode> to) {
        RaftNode val=from.remove(key);
        if(val != null)
            to.putIfAbsent(key, val);
    }

    protected static void moveAll(Map<Address,RaftNode> from, Map<Address,RaftNode> to) {
        for(Map.Entry<Address,RaftNode> e: from.entrySet())
            to.putIfAbsent(e.getKey(), e.getValue());
        from.clear();
    }

    protected static Executor createThreadPool(long max_idle_ms) {
        int max_cores=Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(0, max_cores, max_idle_ms, TimeUnit.MILLISECONDS,
                                      new SynchronousQueue<>());
    }
}
