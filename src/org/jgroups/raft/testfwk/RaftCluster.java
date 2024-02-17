package org.jgroups.raft.testfwk;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

/**
 * Orchestrates a number of {@link RaftNode} objects, to be used in a unit test, for example a leader and a follower.
 * @author Bela Ban
 * @since  1.0.5
 */
public class RaftCluster extends MockRaftCluster {
    // used to 'send' requests between the various instances
    protected final Map<Address,RaftNode> nodes=new ConcurrentHashMap<>();
    protected final Map<Address,RaftNode> dropped_members=new ConcurrentHashMap<>();

    @Override
    public <T extends MockRaftCluster> T add(Address addr, RaftNode node) {
        nodes.put(addr, node);
        return self();
    }

    @Override
    public <T extends MockRaftCluster> T remove(Address addr) {
        nodes.remove(addr);
        return self();
    }

    @Override
    public <T extends MockRaftCluster> T clear() {
        nodes.clear();
        return self();
    }

    public boolean     dropTraffic()                    {return !dropped_members.isEmpty();}
    public RaftCluster dropTrafficTo(Address a)         {move(a, nodes, dropped_members); return this;}
    public RaftCluster clearDroppedTrafficTo(Address a) {move(a, dropped_members, nodes); return this;}
    public RaftCluster clearDroppedTraffic()            {moveAll(dropped_members, nodes); return this;}

    @Override
    public void handleView(View view) {
        List<Address> members=view.getMembers();
        nodes.keySet().retainAll(Objects.requireNonNull(members));
        nodes.values().forEach(n -> n.handleView(view));
    }

    @Override
    public void send(Message msg) {
        send(msg, false);
    }

    @Override
    public int size() {
        return nodes.size();
    }

    public void send(Message msg, boolean async) {
        Address dest=msg.dest();
        boolean block = interceptor != null && interceptor.shouldBlock(msg);

        if(dest != null) {
            // Retrieve the target before possibly blocking.
            RaftNode node=nodes.get(dest);

            // Blocks the invoking thread if cluster is synchronous.
            if (block) {
                interceptor.blockMessage(msg, async, () -> sendSingle(node, msg, async));
            } else {
                sendSingle(node, msg, async);
            }
        } else {
            // Blocks the invoking thread if cluster is synchronous.
            if (block) {
                // Copy the targets before possibly blocking the caller.
                Set<Address> targets = new HashSet<>(nodes.keySet());
                interceptor.blockMessage(msg, async, () -> sendMany(targets, msg, async));
            } else {
                sendMany(nodes.keySet(), msg, async);
            }
        }
    }

    private void sendSingle(RaftNode node, Message msg, boolean async) {
        if(this.async || async)
            deliverAsync(node, msg);
        else
            node.up(msg);
    }

    private void sendMany(Set<Address> targets, Message msg, boolean async) {
        for (Address d : targets) {
            RaftNode n = nodes.get(d);
            if (n == null) continue;

            if(Objects.equals(d, msg.src()) && msg.isFlagSet(DONT_LOOPBACK))
                continue;
            if(this.async || async)
                deliverAsync(n, msg);
            else
                n.up(msg);
        }
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
}
