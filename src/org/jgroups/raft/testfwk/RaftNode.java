package org.jgroups.raft.testfwk;

import org.jgroups.Lifecycle;
import org.jgroups.Message;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Settable;
import org.jgroups.stack.Protocol;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Wraps a {@link org.jgroups.protocols.raft.RAFT} protocol
 * @author Bela Ban
 * @since  1.0.5
 */
public class RaftNode extends Protocol implements Lifecycle, Settable {
    protected final RAFT        raft;    // the wrapped protocol
    protected final RaftCluster cluster;

    public RaftNode(RaftCluster cluster, RAFT r) {
        this.cluster=cluster;
        this.raft=Objects.requireNonNull(r);
        this.raft.setDownProtocol(this);
    }

    public RAFT raft()       {return raft;}

    public void init() throws Exception {
        raft.init();
    }

    public void start() throws Exception {
        raft.start();
    }

    public void stop() {
        raft.stop();
    }

    public void destroy() {
        raft.destroy();
    }


    public Object down(Message msg) {
        msg.setSrc(raft.getAddress());
        cluster.send(msg);
        return null;
    }

    public Object up(Message msg) {
        raft.handleUpRequest(msg, msg.getHeader((raft.getId())));
        return null;
    }

    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length) throws Exception {
        return raft.setAsync(buf, offset, length, null, true);
    }


    public String toString() {
        return String.format("%s [%s]", raft.raftId(), raft);
    }
}
