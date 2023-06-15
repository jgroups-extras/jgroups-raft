package org.jgroups.raft.testfwk;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Lifecycle;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.Options;
import org.jgroups.raft.Settable;
import org.jgroups.stack.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wraps the {@link org.jgroups.protocols.raft.RAFT} and/or {@link org.jgroups.protocols.raft.ELECTION} protocols
 * @author Bela Ban
 * @since  1.0.5
 */
public class RaftNode extends Protocol implements Lifecycle, Settable, Closeable {
    protected final Protocol[]  prots;    // the wrapped protocols, from low to high
    protected final RAFT        raft;
    protected final BaseElection    election;
    protected final MockRaftCluster cluster;

    public RaftNode(MockRaftCluster cluster, Protocol[] protocols) {
        this.cluster=cluster;
        this.prots=Objects.requireNonNull(protocols);
        if(protocols.length == 0)
            throw new IllegalArgumentException("empty protocol list");
        raft=find(RAFT.class);
        BaseElection e = find(ELECTION.class);
        if (e == null) e = find(ELECTION2.class);
        election=e;
        for(int i=prots.length-1; i >= 0; i--) {
            Protocol p=prots[i];
            Protocol below=i -1 >= 0? prots[i-1] : null;
            if(below != null) {
                p.setDownProtocol(below);
                below.setUpProtocol(p);
            }
            else
                p.setDownProtocol(this);
        }
    }

    public RaftNode(RaftCluster cluster, RAFT raft) {
        this(cluster, new Protocol[]{raft});
    }

    public Protocol[] protocols() {return prots;}

    public Address getAddress() {
        for(Protocol p: prots)
            if(p.getAddress() != null)
                return p.getAddress();
        return null;
    }

    public void init() throws Exception {
        for(Protocol p: prots) {
            if(p instanceof RAFT && ((RAFT)p).stateMachine() == null)
                throw new IllegalStateException(String.format("state machine not set in %s", this));
            p.init();
        }
    }

    public void start() throws Exception {
        for(Protocol p: prots)
            p.start();
    }

    public void stop() {
        for(int i=prots.length-1; i >= 0; i--)
            prots[i].stop();
    }

    public void close() throws IOException {
        stop();
    }

    public void destroy() {
        for(int i=prots.length-1; i >= 0; i--)
            prots[i].destroy();
    }

    public void handleView(View v) {
        if(prots != null && prots.length > 0) {
            Protocol top=prots[prots.length-1];
            if(top != null)
                top.down(new Event(Event.VIEW_CHANGE, v));
        }
    }

    public Object down(Event evt) {
        return null;
    }

    public Object down(Message msg) {
        msg.setSrc(localAddress());
        cluster.send(msg);
        return null;
    }

    public Object up(Message msg) {
        // raft.handleUpRequest(msg, msg.getHeader((raft.getId())));
        prots[0].up(msg);
        return null;
    }

    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Options options) throws Exception {
        return raft.setAsync(buf, offset, length, false, options);
    }


    public String toString() {
        return Stream.of(prots)
          .map(p -> String.format("%s [%s]",
                                  p instanceof RAFT? ((RAFT)p).raftId() : p.getAddress(), p))
          .collect(Collectors.joining("\n"));

    }

    protected <T extends Protocol> T find(Class<T> cl) {
        for(Protocol p: prots) {
            if(p.getClass().isAssignableFrom(cl))
                return (T)p;
        }
        return null;
    }

    protected Address localAddress() {
        if(local_addr != null)
            return local_addr;
        for(int i=prots.length-1; i >= 0; i--)
            if((local_addr=prots[i].getAddress()) != null)
                return local_addr;
        return local_addr;
    }

    public RAFT raft() {return raft;}

    public BaseElection election() {return election;}
}
