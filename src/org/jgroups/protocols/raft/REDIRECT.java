package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Protocol that redirects RAFT commands from clients to the actual RAFT leader. E.g. if a client issues a set(), but
 * the current mode is not the leader, the set() is redirected to the leader and the client blocked until the set()
 * has been committed by a majority of nodes.
 * @author Bela Ban
 * @since  0.1
 */
@MBean(description="Redirects requests to current leader")
public class REDIRECT extends Protocol implements Settable, DynamicMembership {
    // When moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short REDIRECT_ID    = 522;

    // When moving to JGroups -> add to jg-magic-map.xml
    protected static final short REDIRECT_HDR   = 4000;

    static {
        ClassConfigurator.addProtocol(REDIRECT_ID,REDIRECT.class);
        ClassConfigurator.add(REDIRECT_HDR, RedirectHeader.class);
    }

    public enum RequestType {SET_REQ, ADD_SERVER, REMOVE_SERVER, RSP};


    protected RAFT                raft;
    protected volatile Address    local_addr;
    protected volatile View       view;
    protected final AtomicInteger request_ids=new AtomicInteger(1);

    // used to correlate redirect requests and responses: keys are request-ids and values futures
    protected final Map<Integer,CompletableFuture<byte[]>> requests=new HashMap<>();


    @Override
    public byte[] set(byte[] buf, int offset, int length) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length);
        return future.get();
    }

    @Override
    public byte[] set(byte[] buf, int offset, int length, long timeout, TimeUnit unit) throws Exception {
        CompletableFuture<byte[]> future=setAsync(buf, offset, length);
        return future.get(timeout, unit);
    }

    @Override
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length) {
        Address leader=leader("set()");

        // we are the current leader: pass the call to the RAFT protocol
        if(Objects.equals(local_addr, leader))
            return raft.setAsync(buf, offset, length);

        // add a unique ID to the request table, so we can correlate the response to the request
        int req_id=request_ids.getAndIncrement();
        CompletableFuture<byte[]> future=new CompletableFuture<>();
        synchronized(requests) {
            requests.put(req_id, future);
        }

        // we're not the current leader -> redirect request to leader and wait for response or timeout
        log.trace("%s: redirecting request %d to leader %s", local_addr, req_id, leader);
        Message redirect=new Message(leader, buf, offset, length)
          .putHeader(id, new RedirectHeader(RequestType.SET_REQ, req_id, false));
        down_prot.down(redirect);
        return future;
    }


    @Override
    public CompletableFuture<byte[]> addServer(String name) throws Exception {
        return changeServer(name, true);
    }

    @Override
    public CompletableFuture<byte[]> removeServer(String name) throws Exception {
        return changeServer(name, false);
    }


    public void init() throws Exception {
        super.init();
        if((raft=RAFT.findProtocol(RAFT.class, this, true)) == null)
            throw new IllegalStateException("RAFT protocol not found");
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                view=evt.getArg();
                break;
        }
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        RedirectHeader hdr=msg.getHeader(id);
        if(hdr != null) {
            handleEvent(msg, hdr);
            return null;
        }
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            RedirectHeader hdr=msg.getHeader(id);
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
            case SET_REQ:
                log.trace("%s: received redirected request %d from %s", local_addr, hdr.corr_id, sender);
                ResponseHandler rsp_handler=new ResponseHandler(sender, hdr.corr_id);
                try {
                    raft.setAsync(msg.getRawBuffer(), msg.getOffset(), msg.getLength())
                      .whenComplete(rsp_handler);
                }
                catch(Throwable t) {
                    rsp_handler.apply(t);
                }
                break;
            case ADD_SERVER:
            case REMOVE_SERVER:
                rsp_handler=new ResponseHandler(sender, hdr.corr_id);
                InternalCommand.Type type=hdr.type == RequestType.ADD_SERVER? InternalCommand.Type.addServer : InternalCommand.Type.removeServer;
                try {
                    raft.changeMembers(new String(msg.getRawBuffer(), msg.getOffset(), msg.getLength()), type)
                      .whenComplete(rsp_handler);
                }
                catch(Throwable t) {
                    rsp_handler.apply(t);
                }
                break;
            case RSP:
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
                            Throwable t=Util.objectFromByteBuffer(msg.getBuffer());
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

    protected Address leader(String req_type) {
        Address leader=raft.leader();
        if(leader == null)
            throw new RuntimeException(String.format("there is currently no leader to forward %s request to", req_type));
        if(view != null && !view.containsMember(leader))
            throw new RuntimeException("leader " + leader + " is not member of view " + view);
        return leader;
    }

    protected CompletableFuture<byte[]> changeServer(String name, boolean add) throws Exception {
        Address leader=leader("addServer()/removeServer()");

        // we are the current leader: pass the call to the RAFT protocol
        if(Objects.equals(local_addr, leader))
            return raft.changeMembers(name, add? InternalCommand.Type.addServer : InternalCommand.Type.removeServer);

        // add a unique ID to the request table, so we can correlate the response to the request
        int req_id=request_ids.getAndIncrement();
        CompletableFuture<byte[]> future=new CompletableFuture<>();
        synchronized(requests) {
            requests.put(req_id, future);
        }

        // we're not the current leader -> redirect request to leader and wait for response or timeout
        log.trace("%s: redirecting request %d to leader %s", local_addr, req_id, leader);
        byte[] buffer=Util.stringToBytes(name);
        Message redirect=new Message(leader, buffer)
          .putHeader(id, new RedirectHeader(add? RequestType.ADD_SERVER : RequestType.REMOVE_SERVER, req_id, false));
        down_prot.down(redirect);
        return future;
    }


    protected class ResponseHandler implements BiConsumer<byte[],Throwable> {
        protected final Address dest;
        protected final int     corr_id;

        public ResponseHandler(Address dest, int corr_id) {
            this.dest=dest;
            this.corr_id=corr_id;
        }

        @Override
        public void accept(byte[] buf, Throwable ex) {
            if(ex != null)
                apply(ex);
            else
                apply(buf);
        }

        protected void apply(byte[] arg) {
            Message msg=new Message(dest, arg).putHeader(id, new RedirectHeader(RequestType.RSP, corr_id, false));
            down_prot.down(msg);
        }

        protected void apply(Throwable t) {
            try {
                byte[] buf=Util.objectToByteBuffer(t);
                Message msg=new Message(dest, buf).putHeader(id, new RedirectHeader(RequestType.RSP, corr_id, true));
                down_prot.down(msg);
            }
            catch(Exception ex) {
                log.error("failed serializing exception", ex);
            }
        }
    }


    public static class RedirectHeader extends Header {
        protected RequestType type;
        protected int         corr_id;   // correlation ID at the sender, so responses can unblock requests (keyed by ID)
        protected boolean     exception; // true if RSP is an exception

        public RedirectHeader() {}

        public RedirectHeader(RequestType type, int corr_id, boolean exception) {
            this.type=type;
            this.corr_id=corr_id;
            this.exception=exception;
        }

        public short getMagicId() {
            return REDIRECT.REDIRECT_HDR;
        }

        public Supplier<? extends Header> create() {
            return RedirectHeader::new;
        }

        public int serializedSize() {
            return Global.BYTE_SIZE*2 + Bits.size(corr_id);
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte((byte)type.ordinal());
            Bits.writeInt(corr_id, out);
            out.writeBoolean(exception);
        }

        public void readFrom(DataInput in) throws Exception {
            type=RequestType.values()[in.readByte()];
            corr_id=Bits.readInt(in);
            exception=in.readBoolean();
        }

        public String toString() {
            return new StringBuilder(type.toString()).append(", corr_id=").append(corr_id)
              .append(", exception=").append(exception).toString();
        }
    }
}
