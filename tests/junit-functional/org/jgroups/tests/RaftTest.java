package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.SHARED_LOOPBACK_PING;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.raft.NO_DUPES;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.SampleStateMachine;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the various stages of the Raft protocoll, e.g. regular append, incorrect append, snapshots, leader change,
 * leader election etc
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RaftTest {
    protected JChannel            a, b, c;
    protected RaftHandle          rha, rhb;
    protected RAFT                raft_a, raft_b, raft_c;
    protected final SampleStateMachine  sma=new SampleStateMachine(),
      smb=new SampleStateMachine(), smc=new SampleStateMachine();
    protected static final String GRP="RaftTest";

    @BeforeMethod
    protected void create() throws Exception {
        a=create("A", 600_000, 1_000_000);
        rha=new RaftHandle(a, sma);
        a.connect(GRP);
        raft_a=raft(a).leader(a.getAddress()).changeRole(Role.Leader);

        b=create("B", 600_000, 1_000_000);
        rhb=new RaftHandle(b, smb);
        b.connect(GRP);
        raft_b=raft(b).leader(a.getAddress()).changeRole(Role.Follower);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
        assert raft_a.isLeader();
        assert !raft_b.isLeader();
    }

    @AfterMethod protected void destroy() throws Exception {
        for(JChannel ch: Arrays.asList(c, b, a)) {
            if(ch == null)
                continue;
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            raft.log().delete();
            Util.close(ch);
        }
    }

    protected static JChannel create(String name, long resend_interval, int max_log_size) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new NO_DUPES(),
          new GMS().setJoinTimeout(1000),
          new FRAG2(),
          // new ELECTION().electionMinInterval(100).electionMaxInterval(300).heartbeatInterval(30),
          new RAFT().members(List.of("A", "B", "C")).raftId(name)
            .logName("rafttest-" + name).resendInterval(resend_interval).maxLogSize(max_log_size),
          new REDIRECT()
        };
        return new JChannel(protocols).name(name);
    }

    public void testSimpleAppend() throws Exception {
        int prev_value=add(rha, 1);
        assert prev_value == 0;
        assert sma.counter() == 1;
        assert smb.counter() == 0; // resend_interval is big, so the commit index on B won't get updated
        assertIndices(1, 1, 0, raft_a);
        assertIndices(1, 0, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 0, 1, 2);

        prev_value=add(rha, 2);
        assert prev_value == 1;
        assert sma.counter() == 3;
        assert smb.counter() == 1; // previous value; B is always lagging one commit behind
        assertCommitTableIndeces(b.getAddress(), raft_a, 1, 2, 3);

        prev_value=add(rha, 3);
        assert prev_value == 3;
        assert sma.counter() == 6;
        assert smb.counter() == 3; // previous value; B is always lagging one commit behind
        assertIndices(3, 3, 0, raft_a);
        assertIndices(3, 2, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 2, 3, 4);

        prev_value=add(rha, -3);
        assert prev_value == 6;
        assert sma.counter() == 3;
        assert smb.counter() == 6; // previous value; B is always lagging one commit behind
        assertIndices(4, 4, 0, raft_a);
        assertIndices(4, 3, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 3, 4, 5);

        for(int i=1,prev=3; i <= 1000; i++) {
            prev_value=add(rha, 5);
            assert prev_value == prev;
            prev+=5;
        }
        assert sma.counter() == 5000 + 3;
        assert smb.counter() == sma.counter() - 5;

        assertIndices(1004, 1004, 0, raft_a);
        assertIndices(1004, 1003, 0, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 1003, 1004, 1005);

        int current_term=raft_a.currentTerm(), expected_term;
        raft_a.currentTerm(expected_term=current_term + 10);

        for(int i=1; i <= 7; i++)
            add(rha, 1);

        assert sma.counter() == 5010;
        assert smb.counter() == sma.counter() - 1;

        assertIndices(1011, 1011, expected_term, raft_a);
        assertIndices(1011, 1010, expected_term, raft_b);
        assertCommitTableIndeces(b.getAddress(), raft_a, 1010, 1011, 1012);
    }

    protected static void assertIndices(int expected_last, int expected_commit, int expected_term, RAFT... rafts) {
        for(RAFT r: rafts) {
            assert r.lastAppended() == expected_last
              : String.format("RAFT.last_appended=%d, expected=%d", r.lastAppended(), expected_last);
            assert r.log().lastAppended() == expected_last
              : String.format("RAFT.log=%s, expected last=%d", r.log(), expected_last);

            assert r.commitIndex() == expected_commit
              : String.format("RAFT.commit=%d, expected=%d", r.commitIndex(), expected_commit);
            assert r.log().commitIndex() == expected_commit
              : String.format("RAFT.log=%s, expected commit=%d", r.commitIndex(), expected_commit);

            assert r.currentTerm() == expected_term
              : String.format("RAFT.term=%d, expected=%d", r.currentTerm(), expected_term);
            assert r.log().currentTerm() == expected_term
              : String.format("RAFT.log=%s, expected term=%d", r.log(), expected_term);
        }
    }

    protected static void assertCommitTableIndeces(Address member, RAFT r, int commit_index, int match_index, int next_index) {
        CommitTable table=r.commitTable();
        assert table != null;
        CommitTable.Entry e=table.get(member);
        assert e.commitIndex() == commit_index : String.format("expected commit_index=%d, entry=%s", commit_index, e);
        assert e.matchIndex() == match_index : String.format("expected match_index=%d, entry=%s", match_index, e);
        assert e.nextIndex() == next_index : String.format("expected next_index=%d, entry=%s", next_index, e);
    }

    protected static int add(RaftHandle rh, int delta) throws Exception {
        byte[] val=new byte[Integer.BYTES];
        Bits.writeInt(delta, val, 0);
        byte[] retval=rh.set(val, 0, val.length, 5, TimeUnit.SECONDS);
        return Bits.readInt(retval, 0);
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }
}
