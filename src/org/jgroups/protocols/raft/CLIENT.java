package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Protocol that redirects RAFT commands from clients to the actual RAFT leader. E.g. if a client issues a set(), but
 * the current mode is not the leader, the set() is redirected to the leader and the client blocked until the set()
 * has been committed by a majority of nodes.
 * @author Bela Ban
 * @since  0.1
 */
@MBean(description="Redirects requests to current leader")
public class CLIENT extends Protocol implements Settable {
    // When moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short CLIENT_ID      = 1026;

    // When moving to JGroups -> add to jg-magic-map.xml
    protected static final short REDIRECT_HDR   = 4000;

    static {
        ClassConfigurator.addProtocol(CLIENT_ID,CLIENT.class);
        ClassConfigurator.add(REDIRECT_HDR, RedirectHeader.class);
    }


    protected RAFT                raft;
    protected volatile Address    local_addr;
    protected volatile View       view;
    protected final AtomicInteger request_ids=new AtomicInteger(1);

    // used to correlate redirect requests and responses: keys are request-ids and values futures
    protected final Map<Integer,CompletableFuture<byte[]>> requests=new HashMap<>();


    @Override
    public byte[] set(byte[] buf, int offset, int length) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length, null);
        return future.get();
    }

    @Override
    public byte[] set(byte[] buf, int offset, int length, long timeout, TimeUnit unit) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length, null);
        return future.get(timeout, unit);
    }

    @Override
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Consumer<byte[]> completion_handler) {
        Address leader=raft.leader();
        if(leader == null)
            throw new RuntimeException("there is currently no leader to forward set() request to");
        if(view != null && !view.containsMember(leader))
            throw new RuntimeException("leader " + leader + " is not member of view " + view);

        // we are the current leader: pass the call to the RAFT protocol
        if(local_addr != null && local_addr.equals(leader))
            return raft.setAsync(buf, offset, length, completion_handler);

        // add a unique ID to the request table, so we can correlate the response to the request
        int req_id=request_ids.getAndIncrement();
        CompletableFuture<byte[]> future=new CompletableFuture<>(completion_handler);
        synchronized(requests) {
            requests.put(req_id, future);
        }

        // we're not the current leader -> redirect request to leader and wait for response or timeout
        log.trace("%s: redirecting request %d to leader %s", local_addr, req_id, leader);
        Message redirect=new Message(leader, buf, offset, length)
          .putHeader(id, new RedirectHeader(RedirectHeader.REQ, req_id, false));
        down_prot.down(new Event(Event.MSG, redirect));
        return future;
    }



    public void init() throws Exception {
        super.init();
        if((raft=RAFT.findProtocol(RAFT.class, this, true)) == null)
            throw new IllegalStateException("RAFT protocol not found");
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                RedirectHeader hdr=(RedirectHeader)msg.getHeader(id);
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


    @Override
    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            RedirectHeader hdr=(RedirectHeader)msg.getHeader(id);
            if(hdr != null) {
                batch.remove(msg);
                handleEvent(msg, hdr);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected void handleEvent(Message msg, RedirectHeader hdr) {
        Address sender=msg.src();
        switch(hdr.type) {
            case RedirectHeader.REQ:
                log.trace("%s: received redirected request %d from %s", local_addr, hdr.corr_id,  sender);
                raft.setAsync(msg.getRawBuffer(), msg.getOffset(), msg.getLength(), new ResponseHandler(sender, hdr.corr_id));
                break;
            case RedirectHeader.RSP:
                CompletableFuture<byte[]> future=null;
                synchronized(requests) {
                    future=requests.remove(hdr.corr_id);
                }
                if(future != null) {
                    log.trace("%s: received response for redirected request %d from %s", local_addr, hdr.corr_id, sender);
                    if(!hdr.exception)
                        future.complete(msg.getBuffer());
                    else {
                        try {
                            Throwable t=(Throwable)Util.objectFromByteBuffer(msg.getBuffer());
                            future.completeExceptionally(t);
                        }
                        catch(Exception e) {
                            log.error("failed deserializing exception", e);
                        }
                    }
                }
                break;
            default:
                log.error("type %d not known", hdr.type);
                break;
        }
    }


    protected class ResponseHandler implements Consumer<byte[]> {
        protected final Address dest;
        protected final int     corr_id;

        public ResponseHandler(Address dest, int corr_id) {
            this.dest=dest;
            this.corr_id=corr_id;
        }

        @Override public void apply(byte[] arg) {
            Message msg=new Message(dest, arg).putHeader(id, new RedirectHeader(RedirectHeader.RSP, corr_id, false));
            down_prot.down(new Event(Event.MSG, msg));
        }

        @Override public void apply(Throwable t) {
            try {
                byte[] buf=Util.objectToByteBuffer(t);
                Message msg=new Message(dest, buf).putHeader(id, new RedirectHeader(RedirectHeader.RSP, corr_id, true));
                down_prot.down(new Event(Event.MSG, msg));
            }
            catch(Exception ex) {
                log.error("failed serializing exception", ex);
            }
        }
    }


    public static class RedirectHeader extends Header {
        protected static final byte REQ = 1;
        protected static final byte RSP = 2;
        protected byte    type;    // REQ or RSP
        protected int     corr_id; // correlation ID at the sender, so responses can unblock requests (keyed by ID)
        protected boolean exception; // true if RSP is an exception

        public RedirectHeader() {}

        public RedirectHeader(byte type, int corr_id, boolean exception) {
            this.type=type;
            this.corr_id=corr_id;
            this.exception=exception;
        }


        public int size() {
            return Global.BYTE_SIZE*2 + Bits.size(corr_id);
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeInt(corr_id, out);
            out.writeBoolean(exception);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            corr_id=Bits.readInt(in);
            exception=in.readBoolean();
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(type == 1? "req" : type == 2? "rsp" : "n/a");
            sb.append(", corr_id=").append(corr_id).append(", exception=").append(exception);
            return sb.toString();
        }
    }
}
