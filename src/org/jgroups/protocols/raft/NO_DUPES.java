package org.jgroups.protocols.raft;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.JoinRsp;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Intercepts JOIN and MERGE requests on the coordinator and rejects members whose addition would lead to members
 * with duplicate raft-ids in the view.</p>
 * Every member's address must be an {@link org.jgroups.util.ExtendedUUID} and have a "raft-id" key whose value
 * is the raft-id. When intercepting a JOIN request whose sender has a raft-id that's already in the view, we send
 * back a {@link org.jgroups.protocols.pbcast.JoinRsp} with a rejection message.</p>
 * Very similar to {@link org.jgroups.protocols.AUTH}.
 * @author Bela Ban
 * @since  0.2
 */
@MBean(description="Rejects views with duplicate members (identical raft-ids)")
public class NO_DUPES extends Protocol {
    protected static final short  gms_id=ClassConfigurator.getProtocolId(GMS.class);
    protected volatile View       view;

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                view=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Message msg) {
        GMS.GmsHeader hdr=msg.getHeader(gms_id);
        if(hdr != null && !handleGmsHeader(hdr, msg.src()))
            return null;
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            GMS.GmsHeader hdr=msg.getHeader(gms_id);
            if(hdr != null && !handleGmsHeader(hdr, msg.src()))
                it.remove();
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /**
     * @return True if the message should be passed up, false if it should be discarded
     */
    protected boolean handleGmsHeader(GMS.GmsHeader hdr, Address sender) {
        switch(hdr.getType()) {
            case GMS.GmsHeader.JOIN_REQ:
            case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                Address joiner=hdr.getMember();
                if(!(joiner instanceof ExtendedUUID)) {
                    log.debug("joiner %s needs to have an ExtendedUUID but has a %s", sender, joiner.getClass().getSimpleName());
                    break;
                }
                View v=view;
                if(contains(v, (ExtendedUUID)joiner)) {
                    String msg=String.format("join of %s rejected as it would create a view with duplicate members (current view: %s)", joiner, v);
                    log.warn(msg);
                    sendJoinRejectedMessageTo(sender, msg);
                    return false;
                }
                break;
            case GMS.GmsHeader.MERGE_REQ:
                // to be done later when we know how to handle merges in jgroups-raft
                break;
        }
        return true;
    }

    protected static boolean contains(View v, ExtendedUUID joiner) {
        byte[] raft_id=joiner.get(RAFT.raft_id_key);
        for(Address addr: v) {
            if(addr instanceof ExtendedUUID) {
                ExtendedUUID uuid=(ExtendedUUID)addr;
                byte[] tmp=uuid.get(RAFT.raft_id_key);
                // compare byte[] buffers to avoid the cost of deserialization
                if(Arrays.equals(raft_id, tmp))
                    return true;
            }
        }
        return false;
    }

    protected void sendJoinRejectedMessageTo(Address joiner, String reject_message) {
        try {
            // needs to be a BytesMessage for now (no ObjectMessage) as GMS itself also uses a BytesMessage;
            // once GMS has been changed in JGroups itself to use an ObjectMessage, we can change this here, too
            ByteArray buffer=Util.streamableToBuffer(new JoinRsp(reject_message));
            Message msg=new BytesMessage(joiner, buffer).putHeader(gms_id, new GMS.GmsHeader(GMS.GmsHeader.JOIN_RSP));
            down_prot.down(msg);
        }
        catch(Exception ex) {
            log.error("failed sending JoinRsp to %s: %s", joiner, ex);
        }
    }

}
