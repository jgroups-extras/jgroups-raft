package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.raft.Options;
import org.jgroups.raft.Settable;
import org.jgroups.raft.internal.metrics.SystemMetricsTracker;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Protocol that redirects RAFT commands from clients to the actual RAFT leader. E.g. if a client issues a set(), but
 * the current mode is not the leader, the set() is redirected to the leader and the client blocked until the set()
 * has been committed by a majority of nodes.
 *
 * @author Bela Ban
 * @since 0.1
 */
@MBean(description = "Redirects requests to current leader")
public class REDIRECT extends Protocol implements Settable, DynamicMembership {
    // When moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short REDIRECT_ID = 522;

    // When moving to JGroups -> add to jg-magic-map.xml
    protected static final short REDIRECT_HDR = 4000;

    static {
        ClassConfigurator.addProtocol(REDIRECT_ID, REDIRECT.class);
        ClassConfigurator.add(REDIRECT_HDR, RedirectHeader.class);
    }

    protected final AtomicInteger request_ids = new AtomicInteger(1);
    // used to correlate redirect requests and responses: keys are request-ids and values futures
    protected final Map<Integer, RedirectRequest> requests = new HashMap<>();
    protected RAFT raft;
    private SystemMetricsTracker systemMetricsTracker;
    protected volatile View view;

    @ManagedAttribute(description = "Mean latency for redirecting request in nanoseconds", type = AttributeType.TIME, unit = TimeUnit.NANOSECONDS)
    public double redirectionOperationMeanLatency() {
        if (systemMetricsTracker == null)
            return -1;

        return systemMetricsTracker.getCommandProcessingLatency().getAvgLatency();
    }

    @Override
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Options options) throws Exception {
        return readWriteAsync(buf, offset, length, options, false);
    }

    @Override
    public CompletableFuture<byte[]> getAsync(byte[] buf, int offset, int length, Options options) throws Exception {
        return readWriteAsync(buf, offset, length, options, true);
    }

    private CompletableFuture<byte[]> readWriteAsync(byte[] buf, int offset, int length, Options options, boolean readOnly) throws Exception {
        Address leader = leader(readOnly ? "get()" : "set()");

        // we are the current leader: pass the call to the RAFT protocol
        if (Objects.equals(local_addr, leader)) {
            return readOnly ? raft.getAsync(buf, offset, length) : raft.setAsync(buf, offset, length);
        }

        // add a unique ID to the request table, so we can correlate the response to the request
        int req_id = request_ids.getAndIncrement();
        RedirectRequest request = new RedirectRequest();
        synchronized (requests) {
            requests.put(req_id, request);
        }

        // we're not the current leader -> redirect request to leader and wait for response or timeout
        log.trace("%s: redirecting request %d to leader %s", local_addr, req_id, leader);
        Message redirect = new BytesMessage(leader, buf, offset, length)
                .putHeader(id, new RedirectHeader(RequestType.REQ, req_id, false).options(options).readOnly(readOnly));
        down_prot.down(redirect);
        return request.future();
    }

    @Override
    public CompletableFuture<byte[]> addServer(String name) throws Exception {
        return changeServer(name, true);
    }

    @Override
    public CompletableFuture<byte[]> removeServer(String name) throws Exception {
        return changeServer(name, false);
    }

    @Override
    public void init() throws Exception {
        super.init();
        if ((raft = RAFT.findProtocol(RAFT.class, this, true)) == null)
            throw new IllegalStateException("RAFT protocol not found");
    }

    @Override
    public void start() throws Exception {
        super.start();

        if (stats && systemMetricsTracker == null)
            systemMetricsTracker = new SystemMetricsTracker();
    }

    @Override
    public Object up(Event evt) {
        if (evt.getType() == Event.VIEW_CHANGE)
            view = evt.getArg();
        return up_prot.up(evt);
    }

    @Override
    public Object up(Message msg) {
        RedirectHeader hdr = msg.getHeader(id);
        if (hdr != null) {
            handleEvent(msg, hdr);
            return null;
        }
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        for (Iterator<Message> it = batch.iterator(); it.hasNext(); ) {
            Message msg = it.next();
            RedirectHeader hdr = msg.getHeader(id);
            if (hdr != null) {
                it.remove();
                handleEvent(msg, hdr);
            }
        }
        if (!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void handleEvent(Message msg, RedirectHeader hdr) {
        switch (hdr.type) {
            case REQ:
                handleUserRequest(msg, hdr);
                break;
            case RSP:
                handleUserResponse(msg, hdr);
                break;
            case ADD_SERVER:
            case REMOVE_SERVER:
                handleMembershipChange(msg, hdr);
                break;
            default:
                log.error("type %d not known", hdr.type);
                break;
        }
    }

    private void handleUserRequest(Message msg, RedirectHeader hdr) {
        Address sender = msg.src();
        Options opts = hdr.options();

        ResponseHandler rsp_handler = new ResponseHandler(sender, hdr.corr_id, opts);
        log.trace("%s: received redirected request %d from %s", local_addr, hdr.corr_id, sender);
        try {
            CompletableFuture<byte[]> cf;
            if (hdr.isReadOnly()) {
                cf = raft.getAsync(msg.getArray(), msg.getOffset(), msg.getLength(), opts);
            } else {
                cf = raft.setAsync(msg.getArray(), msg.getOffset(), msg.getLength(), opts);
            }
            cf.whenComplete(rsp_handler);
        } catch (Throwable t) {
            rsp_handler.apply(t);
        }
    }

    private void handleUserResponse(Message msg, RedirectHeader hdr) {
        RedirectRequest request;
        synchronized (requests) {
            request = requests.remove(hdr.corr_id);
        }

        if (request == null) {
            log.trace("%s: REDIRECT for %d id was not found", local_addr, hdr.corr_id);
            return;
        }

        log.trace("%s: received response for redirected request %d from %s", local_addr, hdr.corr_id, msg.src());
        request.accept(msg, hdr);
    }

    private void handleMembershipChange(Message msg, RedirectHeader hdr) {
        ResponseHandler rsp_handler = new ResponseHandler(msg.src(), hdr.corr_id, Options.create(true));
        InternalCommand.Type type = hdr.type == RequestType.ADD_SERVER
                ? InternalCommand.Type.addServer
                : InternalCommand.Type.removeServer;
        try {
            raft.changeMembers(new String(msg.getArray(), msg.getOffset(), msg.getLength(), StandardCharsets.UTF_8), type)
                    .whenComplete(rsp_handler);
        } catch (Throwable t) {
            rsp_handler.apply(t);
        }
    }

    protected final Address leader(String req_type) throws RaftLeaderException {
        Address leader = raft.leader();
        if (leader == null)
            throw new RaftLeaderException(String.format("there is currently no leader to forward %s request to", req_type));
        if (view != null && !view.containsMember(leader))
            throw new RaftLeaderException("leader " + leader + " is not member of view " + view);
        return leader;
    }

    private CompletableFuture<byte[]> changeServer(String name, boolean add) throws Exception {
        Address leader = leader("addServer()/removeServer()");

        // we are the current leader: pass the call to the RAFT protocol
        if (Objects.equals(local_addr, leader))
            return raft.changeMembers(name, add ? InternalCommand.Type.addServer : InternalCommand.Type.removeServer);

        // add a unique ID to the request table, so we can correlate the response to the request
        int req_id = request_ids.getAndIncrement();
        RedirectRequest request = new RedirectRequest();
        synchronized (requests) {
            requests.put(req_id, request);
        }

        // we're not the current leader -> redirect request to leader and wait for response or timeout
        log.trace("%s: redirecting request %d to leader %s", local_addr, req_id, leader);
        byte[] buffer = Util.stringToBytes(name);
        Message redirect = new BytesMessage(leader, buffer)
                .putHeader(id, new RedirectHeader(add ? RequestType.ADD_SERVER : RequestType.REMOVE_SERVER, req_id, false));
        down_prot.down(redirect);
        return request.future();
    }

    public enum RequestType { REQ, ADD_SERVER, REMOVE_SERVER, RSP }

    public static final class RedirectHeader extends Header {
        private RequestType type;
        private int corr_id;   // correlation ID at the sender, so responses can unblock requests (keyed by ID)
        private boolean exception; // true if RSP is an exception
        private Options options = new Options();
        private boolean readOnly;

        public RedirectHeader() { }

        public RedirectHeader(RequestType type, int corr_id, boolean exception) {
            this.type = type;
            this.corr_id = corr_id;
            this.exception = exception;
        }

        @Override
        public short getMagicId() {
            return REDIRECT_HDR;
        }

        @Override
        public Supplier<? extends Header> create() {
            return RedirectHeader::new;
        }

        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE * 3 + Bits.size(corr_id) + Global.BYTE_SIZE;
        }

        public RedirectHeader options(Options opts) {
            if (opts != null && !options.equals(opts)) this.options = opts;
            return this;
        }

        public Options options() {
            return this.options;
        }

        public boolean isReadOnly() {
            return readOnly;
        }

        public RedirectHeader readOnly(boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte((byte) type.ordinal());
            Bits.writeIntCompressed(corr_id, out);
            out.writeBoolean(exception);
            options.writeTo(out);
            out.writeBoolean(readOnly);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type = RequestType.values()[in.readByte()];
            corr_id = Bits.readIntCompressed(in);
            exception = in.readBoolean();
            options.readFrom(in);
            readOnly = in.readBoolean();
        }

        @Override
        public String toString() {
            return type.toString() + ", corr_id=" + corr_id + ", exception=" + exception;
        }
    }

    private final class RedirectRequest {
        private static final AtomicLongFieldUpdater<RedirectRequest> USER_START_NANOS = AtomicLongFieldUpdater.newUpdater(RedirectRequest.class, "userStartNanos");
        private final CompletableFuture<byte[]> cf;

        private volatile long userStartNanos;

        public RedirectRequest() {
            this.cf = new CompletableFuture<>();
           USER_START_NANOS.set(this, raft.timeService().nanos());
        }

        public void accept(Message msg, RedirectHeader hdr) {
            if (systemMetricsTracker != null) {
                long diff = raft.timeService().interval(USER_START_NANOS.get(this));
                systemMetricsTracker.recordCommandProcessingLatency(diff);
            }

            if (hdr.exception) {
                try {
                    Throwable t = Util.objectFromByteBuffer(msg.getArray());
                    cf.completeExceptionally(t);
                } catch (Exception ex) {
                    log.error("failed serializing exception", ex);
                }
                return;
            }

            byte[] res = hdr.options().ignoreReturnValue() ? null : msg.getArray();
            cf.complete(res);
        }

        public CompletableFuture<byte[]> future() {
            return cf;
        }
    }

    private final class ResponseHandler implements BiConsumer<byte[], Throwable> {
        private final Address dest;
        private final int corr_id;
        private final Options options;

        public ResponseHandler(Address dest, int corr_id, Options opts) {
            this.dest = dest;
            this.corr_id = corr_id;
            this.options = opts;
        }

        @Override
        public void accept(byte[] buf, Throwable ex) {
            if (ex != null)
                apply(ex);
            else
                apply(buf);
        }

        private void apply(byte[] arg) {
            if (arg != null && options != null && options.ignoreReturnValue())
                arg = null;
            Message msg = new BytesMessage(dest, arg)
                    .putHeader(id, new RedirectHeader(RequestType.RSP, corr_id, false).options(options));
            down_prot.down(msg);
        }

        private void apply(Throwable t) {
            try {
                byte[] buf = Util.objectToByteBuffer(t);
                Message msg = new BytesMessage(dest, buf).putHeader(id, new RedirectHeader(RequestType.RSP, corr_id, true));
                down_prot.down(msg);
            } catch (Exception ex) {
                log.error("failed serializing exception", ex);
            }
        }
    }
}
