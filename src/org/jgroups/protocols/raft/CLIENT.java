package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Protocol that redirects RAFT commands from clients to the actual RAFT leader. E.g. if a client issues a set(), but
 * the current mode is not the leader, the set() is redirected to the leader and the client blocked until the set()
 * has been committed by a majority of nodes.
 * @author Bela Ban
 * @since  0.1
 */
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

    // used to correlate redirect requests anfd responses
    protected final Map<Address,Map<Integer,Promise<?>>> requests=new HashMap<Address,Map<Integer,Promise<?>>>();



    public void set(byte[] buf, int offset, int length) {
        Address leader=raft.leader();
        if(leader == null)
            throw new RuntimeException("there is currently no leader to forward set() request to");
        if(view != null && !view.containsMember(leader))
            throw new RuntimeException("leader " + leader + " is not member of view " + view);
        if(local_addr != null && local_addr.equals(leader)) {
            raft.set(buf, offset, length); // we're the current leader, just pass the operation down to RAFT
            return;
        }

        // Redirect request to leader and wait for response or timeout
        int req_id=request_ids.getAndIncrement();
        Promise<?> promise=new Promise<Object>();
        synchronized(requests) {
            if(!requests.containsKey(leader))
                requests.put(leader, new HashMap<Integer,Promise<?>>());
            Map<Integer,Promise<?>> reqs=requests.get(leader);
            reqs.put(req_id, promise);
        }

        // send request to leader

        // wait on promise
        // promise.getResult();
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
            case Event.VIEW_CHANGE:
                view=(View)evt.getArg();
                break;
        }
        return up_prot.up(evt);
    }

    protected static class RedirectHeader extends Header {
        protected byte   type;    // 1=req, 2=rsp
        protected byte[] command; // copied into this header, offset is always 0 and length command.length

        public RedirectHeader() {}

        public RedirectHeader(byte type,byte[] command) {
            this.type=type;
            this.command=command;
        }

        public int size() {
            return Global.BYTE_SIZE + Util.size(command);
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeByteBuffer(command, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            command=Util.readByteBuffer(in);
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(type == 1? "req" : type == 2? "rsp" : "n/a");
            sb.append(command == null? ", empty" : (command.length + " bytes"));
            return sb.toString();
        }
    }
}
