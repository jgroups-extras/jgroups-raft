package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.jgroups.tests.harness.BaseStateMachineTest;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class TimeoutTest extends BaseStateMachineTest<ReplicatedStateMachine<Integer, Integer>> {
    protected static final int                          NUM=200;

    {
        createManually = true;
        recreatePerMethod = true;
    }

    @AfterMethod(alwaysRun = true)
    protected void destroy() throws Exception {
        destroyCluster();
    }

    public void testAppendWithSingleNode() throws Exception {
        _test(1);
    }

    public void testAppendWith3Nodes() throws Exception {
        _test(3);
    }

    protected void _test(int num) throws Exception {
        withClusterSize(num);
        createCluster();

        BooleanSupplier bs = () -> Arrays.stream(channels())
                .map(this::raft)
                .anyMatch(RAFT::isLeader);
        assertThat(eventually(bs, 10, TimeUnit.SECONDS))
                .as("Leader election")
                .isTrue();

        ReplicatedStateMachine<Integer,Integer> sm=null;
        System.out.println("-- waiting for leader");
        for(int i=0; i < channels().length; i++) {
            RAFT raft=raft(i);
            assertThat(raft).isNotNull();
            if(raft.isLeader()) {
                sm=stateMachine(i);
                System.out.printf("-- found leader: %s\n", raft.leader());
                break;
            }
        }

        assert sm != null : "No leader found";
        for(int i=1; i <= NUM; i++) {
            try {
                sm.put(i, i);
            } catch(Exception ex) {
                System.err.printf("put(%d): last-applied=%d, commit-index=%d\n", i, sm.lastApplied(), sm.commitIndex());
                throw ex;
            }
        }

        long start=System.currentTimeMillis();
        sm.allowDirtyReads(false);
        assert sm.get(NUM) == NUM;

        // After reading correctly from the leader with a quorum read, every node should have the same state.
        // We still have to use eventually so the message propagate to ALL nodes, not only majority.
        assertStateMachineEventuallyMatch(IntStream.range(0, num).toArray());
        long time=System.currentTimeMillis()-start;
        System.out.printf("-- it took %d member(s) %d ms to get consistent caches\n", clusterSize, time);

        System.out.print("-- verifying contents of state machines:\n");
        for (int i = 0; i < clusterSize; i++) {
            ReplicatedStateMachine<Integer, Integer> rsm = stateMachine(i);
            System.out.printf("%s: ", rsm.channel().getName());
            for(int j=1; j <= NUM; j++)
                assert rsm.get(j) == j;
            System.out.println("OK");
        }
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.resendInterval(1_000);
    }

    @Override
    protected ReplicatedStateMachine<Integer, Integer> createStateMachine(JChannel ch) {
        ReplicatedStateMachine<Integer, Integer> rsm = new ReplicatedStateMachine<>(ch);
        rsm.timeout(2_000).allowDirtyReads(true);
        return rsm;
    }
}
