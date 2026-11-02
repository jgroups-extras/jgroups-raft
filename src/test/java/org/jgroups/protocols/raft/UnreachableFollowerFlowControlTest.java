package org.jgroups.protocols.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.MFC;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UFC;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.tests.harness.BaseStateMachineTest;
import org.jgroups.raft.util.CounterStateMachine;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

/**
 * A leader replicates to each follower independently (Raft paper, §5.3). A follower that stops answering must not
 * delay the others while a majority is reachable, even when flow control waits for credits that follower never
 * returns. The leader retries lost requests itself, so its requests do not depend on the group's flow control.
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class UnreachableFollowerFlowControlTest extends BaseStateMachineTest<CounterStateMachine> {

    // Small enough for the operations below to use up the credits of the unreachable follower.
    private static final int MAX_CREDITS = 1_000;
    private static final int OPERATIONS = 200;

    {
        clusterSize = 3;
        recreatePerMethod = true;
    }

    @Override
    protected CounterStateMachine createStateMachine(JChannel ch) {
        return new CounterStateMachine();
    }

    @Override
    protected Protocol[] baseProtocolStackForNode(String name) {
        // No failure detection.
        // The unreachable follower stays in the view, as it does until it is suspected.
        // For example, the interval between a node failed and it is detected by the FD.
        return Util.getTestStack(
                new MFC().<MFC>setMaxCredits(MAX_CREDITS),
                new UFC().<UFC>setMaxCredits(MAX_CREDITS),
                createNewRaft(name),
                createRedirect());
    }

    @Override
    protected void afterClusterCreation() throws Exception {
        super.afterClusterCreation();
        for (int i = 0; i < clusterSize; i++) {
            raft(i).setLeaderAndTerm(address(0), 20);
        }
    }

    public void testLeaderServesMajorityWhileFollowerUnreachable() throws Exception {
        // C stops receiving and sending, so it never returns credits to A.
        JChannel channelC = channel(2);
        DISCARD discard = new DISCARD().discardAll(true).setAddress(channelC.getAddress());
        channelC.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);

        try {
            // A and B are a majority, so every write and read completes.
            RaftHandle leader = handle(0);
            for (int i = 1; i <= OPERATIONS; i++) {
                byte[] previous = leader.set(num(1), 0, Integer.BYTES, 5, TimeUnit.SECONDS);
                assertThat(Bits.readInt(previous, 0)).as("value before write %d", i).isEqualTo(i - 1);

                byte[] current = leader.get(num(0), 0, Integer.BYTES, 5, TimeUnit.SECONDS);
                assertThat(Bits.readInt(current, 0)).as("value read after write %d", i).isEqualTo(i);
            }

            RAFT raftA = raft(0);
            RAFT raftB = raft(1);
            assertThat(eventually(() -> raftB.lastAppended() == raftA.lastAppended(), 10, TimeUnit.SECONDS))
                    .as("B has every entry of A")
                    .isTrue();
        } finally {
            // Without DISCARD, C answers the leave requests, and the teardown does not wait for the GMS timeouts.
            channelC.getProtocolStack().removeProtocol(DISCARD.class);
        }
    }

    private static byte[] num(int n) {
        byte[] buf = new byte[Integer.BYTES];
        Bits.writeInt(n, buf, 0);
        return buf;
    }
}
