package org.jgroups.protocols.raft.election;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import org.testng.annotations.Test;

/**
 * @author Zhang Yifei
 * @see <a href="https://github.com/jgroups-extras/jgroups-raft/issues/306">Issue</a>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class NetworkPartitionChannelTest extends BaseRaftElectionTest.ChannelBased {
    private final int[] indexes;
    private final AtomicInteger slowVoteResponses = new AtomicInteger();
    private volatile Semaphore newTerm;

    {
        clusterSize = 5;
        indexes = IntStream.range(0, clusterSize).toArray();
        recreatePerMethod = true;
    }

    public void electionAfterMerge(Class<?> ignore) throws Exception {
        int leader, coord;
        for (; ; ) {
            waitUntilLeaderElected(3000, indexes);
            Address a = leaderAddress();
            leader = index(a);
            // Find a node that address less than leader's
            // DefaultMembershipPolicy will make it to be next coordinator in new membership
            OptionalInt o = stream(indexes).filter(t -> channel(t).address().compareTo(a) < 0).findAny();
            if (o.isPresent()) {
                coord = o.getAsInt();
                break;
            }
            JChannel c = channel(leader);
            c.disconnect();
            c.connect(clusterName());
        }
        assertThat(coordIndex(leader)).isEqualTo(leader);
        LOGGER.info("before partition: {}", view(leader));

        partition(stream(indexes).filter(t -> t != coord).toArray(), new int[]{coord});
        assertThat(coordIndex(leader)).isEqualTo(leader);
        assertThat(coordIndex(coord)).isEqualTo(coord);
        LOGGER.info("partition1: {}", view(leader));
        LOGGER.info("partition2: {}", view(coord));

        raft(leader).set("cmd".getBytes(), 0, 3);
        for (int i : indexes) {
            LOGGER.info("{} lastAppended: {}", address(i), raft(i).lastAppended());
        }

        // block the new coordinator to advance the term in voting thread
        newTerm = new Semaphore(0);

        merge(leader, coord);
        Util.waitUntilAllChannelsHaveSameView(30_000, 1000, channels());
        assertThat(coordIndex(leader)).isEqualTo(coord);
        assertThat(coordIndex(coord)).isEqualTo(coord);
        LOGGER.info("after merge: {}", view(coord));

        // since the term is not advanced yet, new coordinator has accepted the existing leader, and stopping the
        // voting runner, but voting thread is just interrupted, because the voting process almost uninterruptible,
        // so it's still running, the term will be advanced anyway, and the VoteRequest will be sent, if the voting
        // process goes wrong, e.g. waiting response timeout then it won't retry the voting process since the runner
        // has been stopped and the thread has been interrupted.
        waitUntilLeaderElected(3000, indexes);
        LOGGER.info(dumpLeaderAndTerms());

        // slow down the responses, coordinator won't get majority vote responses after waiting timeout
        slowVoteResponses.set(3);

        // unblock the voting thread
        newTerm.release();
        newTerm = null;

        waitUntilPreVoteThreadStops(3000, coord);
        waitUntilVotingThreadStops(3000, coord);

        // ELECTION may be timeout, ELECTION2 always pass.
        waitUntilLeaderElected(3000, indexes);
        LOGGER.info(dumpLeaderAndTerms());
    }

    @Override
    protected RAFT newRaftInstance() {
        return new RAFT() {
            {
                id = ClassConfigurator.getProtocolId(RAFT.class);
            }

            @Override
            public long createNewTerm() {
                Semaphore s = newTerm;
                if (s != null) s.acquireUninterruptibly();
                return super.createNewTerm();
            }
        };
    }

    @Override
    protected Protocol[] baseProtocolStackForNode(String name) throws Exception {
        Protocol[] protocols = super.baseProtocolStackForNode(name);
        protocols[0] = new SHARED_LOOPBACK() {
            @Override
            public Object down(Message msg) {
                if (!addr().equals(msg.dest())) {
                    Header h = msg.getHeader((short) 520);
                    if (h == null) h = msg.getHeader((short) 524);
                    if (h instanceof VoteResponse && slowVoteResponses.getAndDecrement() > 0) park(1000);
                }
                return super.down(msg);
            }
        };
        return protocols;
    }

    private void partition(int[]... partitions) throws TimeoutException {
        List<List<JChannel>> parts = stream(partitions).map(t -> stream(t).mapToObj(this::channel).collect(toList()))
                .collect(toList());
        for (List<JChannel> p : parts) {
            var s = parts.stream().filter(t -> t != p).flatMap(t -> t.stream().map(JChannel::address)).collect(toList());
            p.forEach(t -> t.stack().getBottomProtocol().up(new org.jgroups.Event(org.jgroups.Event.SUSPECT, s)));
            Util.waitUntilAllChannelsHaveSameView(30_000, 1000, p.toArray(JChannel[]::new));
        }
    }

    private void merge(int... coordinators) throws TimeoutException {
        List<JChannel> coords = stream(coordinators).mapToObj(this::channel).collect(toList());
        Map<Address, View> views = coords.stream().collect(toMap(JChannel::address, JChannel::view));
        coords.forEach(t -> t.stack().getBottomProtocol().up(new org.jgroups.Event(org.jgroups.Event.MERGE, views)));
        for (JChannel ch : coords) {
            GMS gms = ch.stack().findProtocol(GMS.class);
            Util.waitUntil(30_000, 1000, () -> !gms.isMergeTaskRunning());
        }
    }

    private View view(int index) {
        return channel(index).stack().<GMS>findProtocol(GMS.class).view();
    }

    private int coordIndex(int index) {
        return index(view(index).getCoord());
    }

    private int index(Address addr) {
        return stream(indexes).filter(t -> channel(t).address().equals(addr)).findAny().getAsInt();
    }

    private void park(int ms) {
        long deadline = System.currentTimeMillis() + ms;
        do {
            LockSupport.parkUntil(deadline);
        } while (System.currentTimeMillis() < deadline);
    }

    void waitUntilPreVoteThreadStops(long timeout, int... indexes) {
        ELECTION2[] a = stream(indexes).mapToObj(this::channel).filter(Objects::nonNull).map(this::election)
                .filter(t -> t instanceof ELECTION2).toArray(ELECTION2[]::new);
        if (a.length == 0) return;
        eventually(() -> {
            for (ELECTION2 e : a) if (e.isPreVoteThreadRunning()) return false;
            return true;
        }, timeout, TimeUnit.MILLISECONDS);
    }
}
