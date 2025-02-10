package org.jgroups.raft.testfwk;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manipulate the cluster during tests.
 * <p>
 * This class offers the possibility of creating partitions in the cluster. The partitions are created by view updates.
 *
 * @since 1.0.12
 * @author José Bolina
 */
public class PartitionedRaftCluster extends MockRaftCluster {
    protected final Map<Address, List<Address>> partitions = new ConcurrentHashMap<>();
    protected final Map<Address, RaftNode> nodes = new ConcurrentHashMap<>();

    private final AtomicBoolean viewChanging = new AtomicBoolean(false);
    private final BlockingQueue<Message> pending = new ArrayBlockingQueue<>(16);

    @Override
    public <T extends MockRaftCluster> T clear() {
        nodes.clear();
        return self();
    }

    @Override
    public <T extends MockRaftCluster> T add(Address addr, RaftNode node) {
        nodes.put(addr, node);
        return self();
    }

    @Override
    public void handleView(View view) {
        viewChanging.set(true);
        try {
            List<Address> members = view.getMembers();
            for (Address member : members) {
                partitions.put(member, members);
            }

            // Update the view in the inverse order.
            // The coordinator usually has additional work in our implementation, therefore, we install
            // the view in the reverse order to make sure all members have the same view.
            for (int i = members.size() - 1; i >= 0; i--) {
                Address member = members.get(i);
                RaftNode node = nodes.get(member);
                node.handleView(view);
            }
        } finally {
            viewChanging.set(false);
            sendPending();
        }
    }

    @Override
    public void send(Message msg) {
        // Enqueue messages during a view change, to make sure everything is sent after the view is installed on all members.
        if (viewChanging.get()) {
            pending.add(msg);
            return;
        }

        Address dest=msg.dest(), src=msg.src();
        boolean block = interceptor != null && interceptor.shouldBlock(msg);

        // Blocks the invoking thread.
        if (block) interceptor.blockMessage(msg);

        if(dest != null) {
            List<Address> connected = partitions.get(src);
            if (connected.contains(dest)) {
                RaftNode node = nodes.get(dest);
                send(node, msg);
            }
        } else {
            Collection<Address> targets = partitions.get(src);
            for (Address a : targets) {
                RaftNode node = nodes.get(a);
                send(node, msg);
            }

            if (!msg.isFlagSet(Message.TransientFlag.DONT_LOOPBACK)) {
                RaftNode node = nodes.get(src);
                send(node, msg);
            }
        }
    }

    @Override
    public int size() {
        return nodes.size();
    }

    @Override
    public <T extends MockRaftCluster> T remove(Address addr) {
        nodes.remove(addr);
        return self();
    }

    private void send(RaftNode node, Message msg) {
        if (async) deliverAsync(node, msg);
        else node.up(msg);
    }

    private void sendPending() {
        Message msg;
        while ((msg = pending.poll()) != null) {
            send(msg);
        }
    }
}
