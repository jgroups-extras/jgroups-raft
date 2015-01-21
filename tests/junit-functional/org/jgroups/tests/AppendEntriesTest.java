package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.raft.ReplicatedStateMachine;
import org.jgroups.protocols.raft.CLIENT;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Tests the AppendEntries functionality: appending log entries in regular operation, new members, late joiners etc
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class AppendEntriesTest {
    protected JChannel                                a,  b,  c;  // A is always the leader
    protected ReplicatedStateMachine<Integer,Integer> as, bs, cs;

    @AfterMethod
    protected void destroy() {
        for(JChannel ch: Arrays.asList(c,b,a)) {
            if(ch == null)
                continue;
            RAFT raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
            raft.log().delete(); // remove log files after the run
        }
        Util.close(c,b,a);
    }




    protected JChannel create(String name, boolean follower) throws Exception {
        ELECTION election=new ELECTION().noElections(follower);
        RAFT raft=new RAFT().majority(2);
        CLIENT client=new CLIENT();
        return new JChannel(Util.getTestStack(election, raft, client)).name(name);
    }
}
