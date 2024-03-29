package org.jgroups.raft.testfwk;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        List<Address> members = view.getMembers();
        for (Address member : members) {
            partitions.put(member, members);
        }

        for (Address member : members) {
            RaftNode node = nodes.get(member);
            node.handleView(view);
        }
    }

    @Override
    public void send(Message msg) {
        Address dest=msg.dest(), src=msg.src();
        boolean block = interceptor != null && interceptor.shouldBlock(msg);

        if(dest != null) {
            // In case the message blocks, we copy the target.
            // This sends a message on the view before the blocking happens.
            List<Address> connected = block
                    ? new ArrayList<>(partitions.get(src))
                    : partitions.get(src);

            // Blocks the invoking thread.
            if (block) interceptor.blockMessage(msg);

            if (connected.contains(dest)) {
                RaftNode node = nodes.get(dest);
                send(node, msg);
            }
        } else {
            // In case the message blocks, we copy the target.
            // This sends a message on the view before the blocking happens.
            Collection<Address> targets = block
                    ? new ArrayList<>(partitions.get(src))
                    : partitions.get(src);

            // Blocks the invoking thread.
            if (block) interceptor.blockMessage(msg);

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
}
