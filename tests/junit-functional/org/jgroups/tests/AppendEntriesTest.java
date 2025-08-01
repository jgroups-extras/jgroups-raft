package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.RaftImpl;
import org.jgroups.protocols.raft.RaftLeaderException;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.tests.harness.BaseStateMachineTest;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

/**
 * Tests the AppendEntries functionality: appending log entries in regular operation, new members, late joiners etc
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class AppendEntriesTest extends BaseStateMachineTest<ReplicatedStateMachine<Integer, Integer>> {

    protected static final byte[]                     buf=new byte[10];
    protected static final int[]                      terms={0,1,1,1,4,4,5,5,6,6,6};

    {
        // We want to customize the cluster size per method. We need to manually create and clear the resources.
        createManually = true;
    }

    @Override
    protected ReplicatedStateMachine<Integer, Integer> createStateMachine(JChannel ch) {
        return new ReplicatedStateMachine<>(ch);
    }

    @AfterMethod(alwaysRun = true)
    void clearResources() throws Exception {
        destroyCluster();
    }

    public void testSingleMember() throws Exception  {
        withClusterSize(1);
        createCluster();

        JChannel a = channel(0);
        RaftHandle raft=new RaftHandle(a, new DummyStateMachine());
        Util.waitUntil(1000, 250, raft::isLeader);

        // when
        byte[] data="foo".getBytes();
        byte[] result=raft.set(data, 0, data.length, 1, SECONDS);

        // then
        assertThat(result)
                .as("should have received empty byte array")
                .isEqualTo(new byte[0]);
    }

    public void testNormalOperation() throws Exception {
        init(true);
        for(int i=1; i <= 10; i++)
            stateMachine(0).put(i, i);

        assertStateMachineEventuallyMatch(0, 1, 2);
        stateMachine(1).remove(5);
        stateMachine(2).put(11, 11);
        stateMachine(2).remove(1);
        stateMachine(0).put(1, 1);
        assertStateMachineEventuallyMatch(0, 1, 2);
    }


    public void testRedirect() throws Exception {
        init(true);
        stateMachine(2).put(5, 5);
        assertStateMachineEventuallyMatch(0, 1, 2);
    }


    public void testPutWithoutLeader() throws Exception {
        withClusterSize(3);
        createCluster(1);

        JChannel a=channel(0); // leader
        assertThat(RaftTestUtils.isRaftLeader(a)).isFalse();
        Assertions.assertThatThrownBy(() -> stateMachine(0).put(1, 1))
                .isInstanceOf(RaftLeaderException.class)
                .hasMessage("there is currently no leader to forward set() request to");
    }


    /**
     * The leader has no majority in the cluster and therefore cannot commit changes. The effect is that all puts on the
     * state machine will fail and the replicated state machine will be empty.
     */
    public void testNonCommitWithoutMajority() throws Exception {
        init(true);
        close(2);
        close(1);

        stateMachine(0).timeout(500);

        BooleanSupplier bs = () -> Arrays.stream(actualChannels())
                .map(this::raft)
                .allMatch(r -> r.leader() == null);
        assertThat(eventually(bs, 5, SECONDS))
                .as(this::dumpLeaderAndTerms)
                .isTrue();

        for(int i=1; i <= 3; i++) {
            int v = i;
            Assertions.assertThatThrownBy(() -> stateMachine(0).put(v, v))
                    .isInstanceOf(RaftLeaderException.class)
                    .hasMessage("there is currently no leader to forward set() request to");
            assertThat(stateMachine(0).size()).isEqualTo(0);
        }
    }

    /**
     * Leader A and followers B and C commit entries 1-2. Then C leaves and A and B commit entries 3-5. When C rejoins,
     * it should get log entries 3-5 as well.
     */
    public void testCatchingUp() throws Exception {
        init(true);

        // A, B and C commit entries 1-2
        for(int i=1; i <= 2; i++)
            stateMachine(0).put(i,i);

        assertStateMachineEventuallyMatch(0, 1, 2);

        // Now C leaves
        close(2);

        // A and B commit entries 3-5
        for(int i=3; i <= 5; i++)
            stateMachine(0).put(i,i);

        assertStateMachineEventuallyMatch(0, 1);

        // Now start C again: entries 1-5 will have to get resent to C as its log was deleted above (otherwise only 3-5
        // would have to be resent)
        System.out.println("-- starting C again, needs to catch up");
        createCluster();
        JChannel c = channel(2);  // follower

        // Now C should also have the same entries (1-5) as A and B
        raft(0).resendInterval(200);
        assertStateMachineEventuallyMatch(0, 1, 2);
    }

    /**
     * Leader A adds the first entry to its log but cannot commit it because it doesn't have a majority (B and C
     * discard all traffic). Then, the DISCARD protocol is removed and the commit entries of B and C should catch up.
     */
    public void testCatchingUpFirstEntry() throws Exception {
        // Create {A,B,C}, A is the leader, then close B and C (A is still the leader)
        init(false);

        // make B and C drop all traffic; this means A won't be able to commit
        for(JChannel ch: Arrays.asList(channel(1), channel(2))) {
            ProtocolStack stack=ch.getProtocolStack();
            DISCARD discard=new DISCARD().discardAll(true).setAddress(ch.getAddress());
            stack.insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
        };

        // Add the first entry, this will time out as there's no majority
        stateMachine(0).timeout(500);
        Assertions.assertThatThrownBy(() -> stateMachine(0).put(1, 1))
                .isInstanceOf(TimeoutException.class);

        RAFT raft=raft(0);
        assertThat(raft)
                .isNotNull()
                .as(String.format("A: last-applied=%d, commit-index=%d\n", raft.lastAppended(), raft.commitIndex()))
                .returns(1L, RAFT::lastAppended)
                .returns(0L, RAFT::commitIndex);

        // now remove the DISCARD protocol from B and C
        Stream.of(channel(1), channel(2)).forEach(ch -> ch.getProtocolStack().removeProtocol(DISCARD.class));

        assertCommitIndex(10000, raft.lastAppended(), raft.lastAppended(), channel(0), channel(1));
        for(JChannel ch: Arrays.asList(channel(0), channel(1))) {
            raft=ch.getProtocolStack().findProtocol(RAFT.class);
            String check = String.format("%s: last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
            System.out.println(check);
            assertThat(raft)
                    .as(check)
                    .returns(1L, RAFT::lastAppended)
                    .returns(1L, RAFT::commitIndex);
        }
        assertStateMachineEventuallyMatch(0, 1, 2);
    }

    /**
     * Tests https://github.com/belaban/jgroups-raft/issues/30-31: correct commit_index after leader restart, and
     * populating request table in RAFT on leader change. Note that the commit index should still be 0, as the change
     * on the leader should throw an exception!
     */
    public void testLeaderRestart() throws Exception {
        withClusterSize(2);

        // A and B now have a majority and A is leader
        createCluster();

        assertThat(RaftTestUtils.eventuallyIsRaftLeader(channel(0), 10_000))
                .as("Channel A never elected")
                .isTrue();
        assertThat(RaftTestUtils.isRaftLeader(channel(1))).isFalse();

        System.out.println("--> disconnecting B");

        close(1); // stop B; it was only needed to make A the leader
        Util.waitUntil(5000, 100, () -> !RaftTestUtils.isRaftLeader(channel(0)));

        // Now try to make a change on A. This will fail as A is not leader anymore
        Assertions.assertThatThrownBy(() -> raft(0).set(new byte[]{'b', 'e', 'l', 'a'}, 0, 4, 500, TimeUnit.MILLISECONDS))
                .as("Trying to set value without leader")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("I'm not the leader ");

        // A now has last_applied=1 and commit_index=0:
        assertCommitIndex(10000, 0, 0, channel(0));

        // Now start B again, this gives us a majority and entry #1 should be able to be committed
        System.out.println("--> restarting B");

        // A and B now have a majority and A is leader
        createCluster();

        // A and B should now have last_applied=0 and commit_index=0
        assertCommitIndex(10000, 0, 0, channel(0), channel(1));
    }


    /**
     * Leader A and follower B commit 5 entries, then snapshot A. Then C comes up and should get the 5 committed entries
     * as well, as a snapshot
     */
    public void testInstallSnapshotInC() throws Exception {
        init(true);
        close(2);

        for(int i=1; i <= 5; i++)
            stateMachine(0).put(i,i);
        assertStateMachineEventuallyMatch(0, 1);

        // Snapshot A:
        stateMachine(0).snapshot();

        // Now start C
        createCluster();  // follower

        assertStateMachineEventuallyMatch(0, 1, 2);
    }


    /** Tests an append at index 1 with prev_index 0 and index=2 with prev_index=1 */
    public void testInitialAppends() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();

        AppendResult result=append(impl, 1, 0, new LogEntry(4, buf), leader, 1);
        assertThat(result)
                .as("Validating: " + result)
                .returns(true, AppendResult::success)
                .returns(1L, AppendResult::index)
                .returns(1L, AppendResult::commitIndex);
        assertLogIndices(log, 1, 1, 4);

        result=append(impl, 2, 4, new LogEntry(4, buf), leader, 1);
        assertThat(result)
                .as("Validating: " + result)
                .returns(true, AppendResult::success)
                .returns(2L, AppendResult::index)
                .returns(1L, AppendResult::commitIndex);
        assertLogIndices(log, 2, 1, 4);

        result=append(impl, 2, 4, new LogEntry(4, null), leader, 2);
        assertThat(result)
                .as("Validating: " + result)
                .returns(true, AppendResult::success)
                .returns(2L, AppendResult::index)
                .returns(2L, AppendResult::commitIndex);
        assertLogIndices(log, 2, 2, 4);
    }

    /** Tests append _after_ the last appended index; returns an AppendResult with index=last_appended */
    public void testAppendAfterLastAppended() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();

        // initial append at index 1
        AppendResult result=append(impl, 1, 0, new LogEntry(4, buf), leader, 1);
        assertThat(result)
                .as("Validating: " + result)
                .returns(true, AppendResult::success)
                .returns(1L, AppendResult::index);
        assertLogIndices(log, 1, 1, 4);

        // append at index 3 fails because there is no entry at index 2
        result=append(impl, 3, 4, new LogEntry(4, buf), leader, 1);
        assertThat(result)
                .as("Validating: " + result)
                .returns(false, AppendResult::success)
                .returns(1L, AppendResult::index);
        assertLogIndices(log, 1, 1, 4);
    }

    public void testSendCommitsImmediately() throws Exception {
        // Force leader to send commit messages immediately
        init(true);
        leader().resendInterval(60_000).sendCommitsImmediately(true);
        System.out.println("-- sending value");
        stateMachine(0).put(1,1);
        System.out.println("-- finished operation, waiting for broadcast");
        assertStateMachineEventuallyMatch(0, 1, 2);
    }

    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Log    01 01 01 04 04 05 05 06 06 06
    // Append                            07 <--- wrong prev_term at index 11
    public void testAppendWithConflictingTerm() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();

        for(int i=1; i <= 10; i++)
            append(impl,  i, terms[i-1], new LogEntry(terms[i], buf), leader, 1);

        // now append(index=11,term=7) -> should return false result with index=8
        AppendResult result=append(impl, 11, 7, new LogEntry(6, buf), leader, 1);
        assertThat(result)
                .satisfies(r -> assertThat(r.success()).isFalse())
                .satisfies(r -> assertThat(r.index()).isEqualTo(8))
                .satisfies(r -> assertThat(r.nonMatchingTerm()).isEqualTo(6));
        assertLogIndices(log, 7, 1, 5);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Log    01
    // Append    07 <--- wrong prev_term at index 1
    public void testAppendWithConflictingTerm2() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();

        append(impl,  1, 0, new LogEntry(1, buf), leader, 0);

        // now append(index=2,term=7) -> should return false result with index=1
        AppendResult result=append(impl, 2, 7, new LogEntry(7, buf), leader, 1);
        assertThat(result)
                .satisfies(r -> assertThat(r.success()).isFalse())
                .satisfies(r -> assertThat(r.index()).isEqualTo(1))
                .satisfies(r -> assertThat(r.nonMatchingTerm()).isEqualTo(1));
        assertLogIndices(log, 0, 0, 0);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Log    01 03 05
    // Append          07 <--- wrong prev_term at index 3
    public void testAppendWithConflictingTerm3() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();

        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(3, buf), leader, 1);
        append(impl,  3, 3, new LogEntry(5, buf), leader, 1);

        // now append(index=2,term=7) -> should return false result with index=1
        AppendResult result=append(impl, 4, 7, new LogEntry(7, buf), leader, 1);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(3L, AppendResult::index)
                .returns(5L, AppendResult::nonMatchingTerm);
        assertLogIndices(log, 2, 1, 3);
    }


    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    public void testRAFTPaperAppendOnLeader() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 10);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 1);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(true, AppendResult::success)
                .returns(11L, AppendResult::index);
        assertLogIndices(log, 11, 10, 6);
    }

    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06    06 <-- add
    public void testRAFTPaperScenarioA() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 9);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 9);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(false, AppendResult::success)
                .returns(9L, AppendResult::index);
        assertLogIndices(log, 9, 9, 6);
    }


    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04                   06 <-- add
    public void testRAFTPaperScenarioB() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl, 2, 1, new LogEntry(1, buf), leader, 1);
        append(impl, 3, 1, new LogEntry(1, buf), leader, 1);
        append(impl, 4, 1, new LogEntry(4, buf), leader, 4);
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 4);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(false, AppendResult::success)
                .returns(4L, AppendResult::index);
        assertLogIndices(log, 4, 4, 4);
    }

    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06 06 06
    public void testRAFTPaperScenarioC() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 11, 6, new LogEntry(6, buf), leader, 10);
        // Overwrites existing entry; does *not* advance last_applied in log
        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 10);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(true, AppendResult::success)
                .returns(11L, AppendResult::index);
        assertLogIndices(log, 11, 10, 6);
    }


    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 05 05 06 06 06 07 07
    public void testRAFTPaperScenarioD() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(5, buf), leader, 1);
        append(impl,  7, 5, new LogEntry(5, buf), leader, 1);
        append(impl,  8, 5, new LogEntry(6, buf), leader, 1);
        append(impl,  9, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 10, 6, new LogEntry(6, buf), leader, 1);
        append(impl, 11, 6, new LogEntry(7, buf), leader, 1);
        append(impl, 12, 7, new LogEntry(7, buf), leader, 10);

        // add 11
        AppendResult result=append(impl, buf, leader, 10, 6, 8, 10);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(true, AppendResult::success)
                .returns(11L, AppendResult::index);
        assertLogIndices(log, 11, 10, 8);

        // add 12
        result=append(impl, buf, leader, 11, 8, 8, 10);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(true, AppendResult::success)
                .returns(12L, AppendResult::index);
        assertLogIndices(log, 12, 10, 8);

        // commit 12
        result=append(impl, null, leader, 0, 0, 0, 12);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(true, AppendResult::success)
                .returns(12L, AppendResult::index);
        assertLogIndices(log, 12, 12, 8);
    }

    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 04 04 04 04
    public void testRAFTPaperScenarioE() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();
        append(impl,  1, 0, new LogEntry(1, buf), leader, 1);
        append(impl,  2, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  3, 1, new LogEntry(1, buf), leader, 1);
        append(impl,  4, 1, new LogEntry(4, buf), leader, 1);
        append(impl,  5, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  6, 4, new LogEntry(4, buf), leader, 1);
        append(impl,  7, 4, new LogEntry(4, buf), leader, 3);

        System.out.printf("log entries of follower before fix:\n%s", printLog(log));

        AppendResult result=append(impl, 11, 6, new LogEntry(6, buf), leader, 10);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(false, AppendResult::success)
                .returns(7L, AppendResult::index);
        assertLogIndices(log, 7, 3, 4);

        // now try to append 8 (fails because of wrong term)
        result=append(impl, 8, 5, new LogEntry(6, buf), leader, 10);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(false, AppendResult::success)
                .returns(4L, AppendResult::index)
                .returns(3L, AppendResult::commitIndex);
        assertLogIndices(log, 3, 3, 1);

        // now append 4-10
        for(int i=4; i <= 10; i++) {
            result=append(impl, i, terms[i-1], new LogEntry(terms[i], buf), leader, 10);
            assertThat(result)
                    .as("Verifying: " + result)
                    .returns(true, AppendResult::success)
                    .returns((long) i, AppendResult::index)
                    .returns((long) i, AppendResult::commitIndex);
            assertLogIndices(log, i, i, terms[i]);
        }
        System.out.printf("log entries of follower after fix:\n%s", printLog(log));
        for(int i=0; i < terms.length; i++) {
            LogEntry entry=log.get(i);
            if (i == 0) assertThat(entry).isNull();
            else assertThat(entry).as("Verifying: " + entry).isNotNull().returns((long) terms[i], LogEntry::term);
        }
    }


    // 5.3, fig. 7
    // Index  01 02 03 04 05 06 07 08 09 10 11 12
    // Leader 01 01 01 04 04 05 05 06 06 06
    // Flwr A 01 01 01 02 02 02 03 03 03 03 03
    public void testRAFTPaperScenarioF() throws Exception {
        initB();
        Address leader = leaderAddress();
        RaftImpl impl=getImpl(channel(1));
        Log log=impl.raft().log();
        int[] incorrect_terms={0,1,1,1,2,2,2,3,3,3,3,3};
        for(int i=1; i < incorrect_terms.length; i++)
            append(impl, i, incorrect_terms[i-1], new LogEntry(incorrect_terms[i], buf), leader, 3);

        System.out.printf("log entries of follower before fix:\n%s", printLog(log));

        AppendResult result=append(impl, 10, 6, new LogEntry(6, buf), leader, 10);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(false, AppendResult::success)
                .returns(7L, AppendResult::index);
        assertLogIndices(log, 6, 3, 2);

        System.out.printf("log entries of follower after first fix:\n%s", printLog(log));

        result=append(impl, 7, 5, new LogEntry(5, buf), leader, 10);
        assertThat(result)
                .as("Verifying: " + result)
                .returns(false, AppendResult::success)
                .returns(4L, AppendResult::index);
        assertLogIndices(log, 3, 3, 1);

        System.out.printf("log entries of follower after second fix:\n%s", printLog(log));

        // now append 4-10:
        for(int i=4; i <= 10; i++) {
            result=append(impl, i, terms[i-1], new LogEntry(terms[i], buf), leader, 10);
            assertThat(result)
                    .as("Verifying: " + result)
                    .returns(true, AppendResult::success)
                    .returns((long) i, AppendResult::index)
                    .returns((long) i, AppendResult::commitIndex);
            assertLogIndices(log, i, i, terms[i]);
        }
        System.out.printf("log entries of follower after final fix:\n%s", printLog(log));
        for(int i=0; i < terms.length; i++) {
            LogEntry entry=log.get(i);
            if (i == 0) assertThat(entry).isNull();
            else assertThat(entry).isNotNull().returns((long) terms[i], LogEntry::term);
        }
    }

    protected void init(boolean verbose) throws Exception {
        withClusterSize(3);
        createCluster();

        Util.waitUntil(5000, 100,
                       () -> Stream.of(channels()).map(this::raft).allMatch(r -> r.leader() != null),
                       () -> Stream.of(channels()).map(ch -> String.format("%s: leader=%s", ch.getAddress(), raft(ch).leader()))
                         .collect(Collectors.joining("\n")));

        if(verbose) {
            System.out.println("A: is leader? -> " + RaftTestUtils.isRaftLeader(channel(0)));
            System.out.println("B: is leader? -> " + RaftTestUtils.isRaftLeader(channel(1)));
            System.out.println("C: is leader? -> " + RaftTestUtils.isRaftLeader(channel(2)));
        }

        assertThat(RaftTestUtils.isRaftLeader(channel(0))).isTrue();
        assertThat(RaftTestUtils.isRaftLeader(channel(1))).isFalse();
        assertThat(RaftTestUtils.isRaftLeader(channel(2))).isFalse();
    }

    protected void initB() throws Exception {
        withClusterSize(2);
        createCluster();
        close(0);
    }

    protected static RaftImpl getImpl(JChannel ch) {
        RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
        return raft.impl();
    }

    protected static String printLog(Log l) {
        StringBuilder sb=new StringBuilder();
        l.forEach((e,i) -> sb.append(String.format("%d: term=%d\n", i, e.term())));
        return sb.toString();
    }

    protected static void assertLogIndices(Log log, int last_appended, int commit_index, int term) {
        assertThat(eventually(() -> log.lastAppended() == last_appended, 5, SECONDS)).isTrue();
        assertThat(eventually(() -> log.commitIndex() == commit_index, 5, SECONDS)).isTrue();
        assertThat(eventually(() -> log.currentTerm() == term, 5, SECONDS)).isTrue();
    }

    protected void assertCommitIndex(long timeout, long expected_commit, long expected_applied, JChannel... channels) {
        BooleanSupplier bs = () -> {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                RAFT raft=raft(ch);
                if(expected_commit != raft.commitIndex() || expected_applied != raft.lastAppended())
                    all_ok=false;
            }
            return all_ok;
        };
        assertThat(eventually(bs, timeout, TimeUnit.MILLISECONDS))
                .as("Commit indexes never matched")
                .isTrue();

        for(JChannel ch: channels) {
            RAFT raft=raft(ch);
            String check = String.format("%s: last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
            System.out.printf(check);
            assertThat(raft)
                    .as(check)
                    .returns(expected_commit, RAFT::commitIndex)
                    .returns(expected_applied, RAFT::lastAppended);
        }
    }

    protected static AppendResult append(RaftImpl impl, long index, long prev_term, LogEntry entry, Address leader,
                                         long leader_commit) throws Exception {
        return append(impl, entry.command(), leader, Math.max(0, index-1), prev_term, entry.term(), leader_commit);
    }

    protected static AppendResult append(RaftImpl impl, byte[] data, Address leader,
                                         long prev_log_index, long prev_log_term, long entry_term, long leader_commit) throws Exception {
        int len=data != null? data.length : 0;
        LogEntries entries=new LogEntries().add(new LogEntry(entry_term, data, 0, len));
        return impl.handleAppendEntriesRequest(entries, leader, prev_log_index, prev_log_term, entry_term, leader_commit);
    }

}
