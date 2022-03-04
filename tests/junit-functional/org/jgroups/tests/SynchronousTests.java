package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.raft.testfwk.RaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.util.Bits;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Same as {@link RaftTest}, but only a single thread is used to run the tests (no asynchronous processing).
 * This allows for simple stepping-through in a debugger, without a thread handing off the processing to another
 * thread. Simplifies observing changes to the state machines represented by leaders and followers.
 * @author Bela Ban
 * @since  1.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class SynchronousTests {
    protected final Address       a=createAddress("A"), b=createAddress("B");
    protected final View          view=View.create(a, 1, a,b);
    protected final List<String>  mbrs=List.of("A", "B");
    protected final RaftCluster   cluster=new RaftCluster();
    protected RAFT                raft_a, raft_b;
    protected RaftNode            node_a, node_b;
    protected StateMachine        sma, smb;

    @BeforeMethod protected void init() throws Exception {
        raft_a=createRAFT(a, "A", mbrs).stateMachine(sma=new CounterStateMachine())
          .changeRole(Role.Leader).leader(a);
        raft_b=createRAFT(b, "B", mbrs).stateMachine(smb=new CounterStateMachine())
          .changeRole(Role.Follower).leader(a);
        node_a=new RaftNode(cluster, raft_a);
        node_b=new RaftNode(cluster, raft_b);
        node_a.init();
        node_b.init();
        cluster.add(a, node_a).add(b, node_b);
        cluster.handleView(view);
        node_a.start();
        node_b.start();
        raft_a.currentTerm(5);
    }


    @AfterMethod
    protected void destroy() throws Exception {
        node_b.stop();
        node_a.stop();
        node_b.destroy();
        node_a.destroy();
        raft_a.deleteLog();
        raft_a.deleteSnapshot();
        raft_b.deleteLog();
        raft_b.deleteSnapshot();
        cluster.clear();
    }


    public void testSimpleAppend() throws Exception {
        byte[] buf=number(1);
        byte[] retval=node_a.set(buf, 0, buf.length);
        System.out.println("retval = " + Arrays.toString(retval));

        for(;;) {
            CompletableFuture<byte[]> f=node_a.setAsync(buf, 0, buf.length);
            f.whenComplete((r,ex) -> System.out.printf("result=%d, ex=%s\n", Bits.readInt(r, 0), ex));
            Util.sleep(1000);
        }
    }


    protected static RAFT createRAFT(Address addr, String name, List<String> members) {
        return new RAFT().raftId(name).members(members).logName("synctest-" + name)
          .resendInterval(600_000) // long to disable resending by default
          .setAddress(addr);
    }

    protected static Address createAddress(String name) {
        ExtendedUUID.setPrintFunction(RAFT.print_function);
        return ExtendedUUID.randomUUID(name).put(RAFT.raft_id_key, Util.stringToBytes(name));
    }

    protected static byte[] number(int n) {
        byte[] buf=new byte[Integer.BYTES];
        Bits.writeInt(n, buf, 0);
        return buf;
    }



    /*public void testRegularAppend() throws Exception {
        int prev_value=add(rha, 1);
        expect(0, prev_value);
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
    }*/
}
