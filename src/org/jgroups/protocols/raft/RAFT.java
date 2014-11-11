package org.jgroups.protocols.raft;

import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;

/**
 * Implementation of the RAFT consensus protocol in JGroups
 * @author Bela Ban
 * @since  3.6
 */
@MBean(description="Implementation of the RAFT consensus protocol")
public class RAFT extends Protocol {

    // todo: when moving to JGroups -> add to jg-protocol-ids.xml
    protected static final short RAFT_ID              = 1024;

    // todo: when moving to JGroups -> add to jg-magic-map.xml
    protected static final short APPEND_ENTRIES_HDR   =  200;
    protected static final short REQUEST_VOTE_HDR     =  201;
    protected static final short INSTALL_SNAPSHOT_HDR =  202;
    static {
        ClassConfigurator.addProtocol(RAFT_ID, RAFT.class);
        ClassConfigurator.add(APPEND_ENTRIES_HDR,   AppendEntriesHeader.class);
        ClassConfigurator.add(REQUEST_VOTE_HDR,     RequestVoteHeader.class);
        ClassConfigurator.add(INSTALL_SNAPSHOT_HDR, InstallSnapshotHeader.class);
    }
}
