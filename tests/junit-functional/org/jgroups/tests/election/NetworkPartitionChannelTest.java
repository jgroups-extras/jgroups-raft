package org.jgroups.tests.election;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.protocols.raft.election.VoteResponse;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;
import static org.jgroups.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;
import static org.testng.Assert.assertEquals;

/**
 * @author Zhang Yifei
 * @see <a href="https://github.com/jgroups-extras/jgroups-raft/issues/306">Issue</a>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class NetworkPartitionChannelTest extends BaseRaftElectionTest.ChannelBased {
	private final int[] indexes;
	private volatile Semaphore newTerm;

	{
		clusterSize = 5;
		indexes = IntStream.range(0, clusterSize).toArray();
		recreatePerMethod = true;
	}

	public void electionAfterMerge(Class<?> ignore) throws Exception {
		int leader, coord;
		for (;;) {
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
		assertEquals(coordIndex(leader), leader);
		System.out.println("before partition: " + view(leader));

		partition(stream(indexes).filter(t -> t != coord).toArray(), new int[] {coord});
		assertEquals(coordIndex(leader), leader);
		assertEquals(coordIndex(coord), coord);
		System.out.println("partition1: " + view(leader));
		System.out.println("partition2: " + view(coord));

		// block the new coordinator to advance the term in voting thread
		newTerm = new Semaphore(0);

		merge(leader, coord);

		// since the term is not advanced yet, new coordinator has accepted the existing leader, and stopping the
		// voting thread, but voting thread is just interrupted, it's still running, the term will be advanced anyway,
		// and the VoteRequest will be sent, if the voting process goes wrong, e.g. waiting response timeout then it
		// won't retry the voting process since the thread has been interrupted.
		waitUntilLeaderElected(3000, indexes);
		System.out.println(dumpLeaderAndTerms());

		// try to make waiting VoteResponse timeout
		election(channel(coord)).voteTimeout(1);

		// unblock the voting thread
		newTerm.release();
		newTerm = null;

		int finalLeader = leader;
		assertThat(eventually(() -> coordIndex(finalLeader) == coord && coordIndex(coord) == coord, 10, TimeUnit.SECONDS))
				.isTrue();
		System.out.println("after merge: " + view(coord));

		// ELECTION may be timeout, ELECTION2 always pass.
		waitUntilLeaderElected(5000, indexes);
		System.out.println(dumpLeaderAndTerms());
	}

	@Override
	protected RAFT newRaftInstance() {
		return new RAFT() {
			@Override
			public long createNewTerm() {
				Semaphore s = newTerm;
				if (s != null) s.acquireUninterruptibly();
				return super.createNewTerm();
			}
		};
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
		GMS gms = channel(index).stack().findProtocol(GMS.class);
		return gms.view();
	}

	private int coordIndex(int index) {
		return index(view(index).getCoord());
	}

	private int index(Address addr) {
		return stream(indexes).filter(t -> channel(t).address().equals(addr)).findAny().getAsInt();
	}
}
