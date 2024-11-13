package org.jgroups.tests.blocks;

import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;
import static org.jgroups.raft.blocks.LockService.LockStatus.HOLDING;
import static org.jgroups.raft.blocks.LockService.LockStatus.NONE;
import static org.jgroups.raft.blocks.LockService.LockStatus.WAITING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.data.Offset;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.Options;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.blocks.LockService;
import org.jgroups.raft.blocks.LockService.LockStatus;
import org.jgroups.raft.blocks.LockService.Mutex;
import org.jgroups.tests.harness.BaseRaftChannelTest;
import org.jgroups.tests.harness.BaseRaftElectionTest;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * @author Zhang Yifei
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class LockServiceTest extends BaseRaftChannelTest {

	protected Service service_a, service_b, service_c, service_d, service_e;
	protected Events events_a, events_b, events_c, events_d, events_e;

	{
		clusterSize = 5;
		recreatePerMethod = true;
	}

	protected static class Event {
		final long key; final LockStatus prev, curr;
		Event(long key, LockStatus prev, LockStatus curr) { this.key = key; this.prev = prev; this.curr = curr; }

		protected void assertEq(long key, LockStatus prev, LockStatus curr) {
			assertThat(this).usingRecursiveComparison().isEqualTo(new Event(key, prev, curr));
		}
	}

	protected static class Events implements LockService.Listener {
		final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();

		@Override
		public void onStatusChange(long key, LockStatus prev, LockStatus curr) {
			queue.offer(new Event(key, prev, curr));
		}

		protected Event next(int secs) throws InterruptedException { return queue.poll(secs, SECONDS); }
		protected Event next() throws InterruptedException { return next(3); }
	}

	protected static class Service extends LockService {
		TestRaft raft;

		public Service(JChannel channel) { super(channel); }

		@Override
		protected RaftHandle createRaft(JChannel ch, StateMachine sm) { return raft = new TestRaft(ch, sm); }

		Map<Long, List<UUID>> dumpState() {
			assert locks.values().stream().filter(t -> t.holder != null)
					.flatMap(t -> concat(Stream.of(t.holder), t.waiters.stream()).map(m -> Map.entry(m, t)))
					.collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toSet()))).equals(memberLocks);
			return locks.values().stream().filter(t -> t.holder != null)
					.collect(Collectors.toMap(t -> t.id, t -> {
						List<UUID> list = new ArrayList<>(t.waiters.size() + 1);
						list.add(t.holder); list.addAll(t.waiters); return list;
					}));
		}
	}

	protected static class TestRaft extends RaftHandle {
		Callable<CompletableFuture<byte[]>> interceptor;

		public TestRaft(JChannel ch, StateMachine sm) { super(ch, sm); }

		@Override
		public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length, Options options) throws Exception {
			if (interceptor != null) return interceptor.call();
			return super.setAsync(buf, offset, length, options);
		}

		void throwingInterceptor(Exception e) { interceptor = () -> { throw e; }; }
		void errorInterceptor(Throwable e) { interceptor = () -> CompletableFuture.failedFuture(e); }
		void voidInterceptor() { interceptor = CompletableFuture::new; }
		void removeInterceptor() { interceptor = null; }
	}

	@Override
	protected void beforeChannelConnection(JChannel ch) {
		switch (ch.name()) {
			case "A": service_a = new Service(ch); break;
			case "B": service_b = new Service(ch); break;
			case "C": service_c = new Service(ch); break;
			case "D": service_d = new Service(ch); break;
			case "E": service_e = new Service(ch); break;
		}
	}

	@Override
	protected void afterClusterCreation() {
		RAFT[] rafts = stream(channels()).map(this::raft).toArray(RAFT[]::new);
		BaseRaftElectionTest.waitUntilAllHaveLeaderElected(rafts, 15_000);
	}

	protected void enableEvents() {
		service_a.addListener(events_a = new Events());
		service_b.addListener(events_b = new Events());
		service_c.addListener(events_c = new Events());
		service_d.addListener(events_d = new Events());
		service_e.addListener(events_e = new Events());
	}

	public void lock() throws Exception {
		enableEvents();

		// lock 101
		assertEquals(service_a.lock(101L).get(3, SECONDS), HOLDING);
		events_a.next().assertEq(101L, NONE, HOLDING);
		assertEquals(service_a.lockStatus(101L), HOLDING);

		assertEquals(service_b.lock(101L).get(3, SECONDS), WAITING);
		events_b.next().assertEq(101L, NONE, WAITING);
		assertEquals(service_b.lockStatus(101L), WAITING);

		assertEquals(service_c.lock(101L).get(3, SECONDS), WAITING);
		events_c.next().assertEq(101L, NONE, WAITING);
		assertEquals(service_c.lockStatus(101L), WAITING);

		// lock 102
		assertEquals(service_b.lock(102L).get(3, SECONDS), HOLDING);
		events_b.next().assertEq(102L, NONE, HOLDING);
		assertEquals(service_b.lockStatus(102L), HOLDING);

		assertEquals(service_a.lock(102L).get(3, SECONDS), WAITING);
		events_a.next().assertEq(102L, NONE, WAITING);
		assertEquals(service_a.lockStatus(102L), WAITING);

		assertEquals(service_c.lock(102L).get(3, SECONDS), WAITING);
		events_c.next().assertEq(102L, NONE, WAITING);
		assertEquals(service_c.lockStatus(102L), WAITING);

		// lock 103
		assertEquals(service_c.lock(103L).get(3, SECONDS), HOLDING);
		events_c.next().assertEq(103L, NONE, HOLDING);
		assertEquals(service_c.lockStatus(103L), HOLDING);

		assertEquals(service_a.lock(103L).get(3, SECONDS), WAITING);
		events_a.next().assertEq(103L, NONE, WAITING);
		assertEquals(service_a.lockStatus(103L), WAITING);

		assertEquals(service_b.lock(103L).get(3, SECONDS), WAITING);
		events_b.next().assertEq(103L, NONE, WAITING);
		assertEquals(service_b.lockStatus(103L), WAITING);

		// unlock 101
		service_a.unlock(101L).get(3, SECONDS);

		events_a.next().assertEq(101L, HOLDING, NONE);
		assertEquals(service_a.lockStatus(101L), NONE);

		events_b.next().assertEq(101L, WAITING, HOLDING);
		assertEquals(service_b.lockStatus(101L), HOLDING);

		// unlock 102
		service_b.unlock(102L).get(3, SECONDS);

		events_b.next().assertEq(102L, HOLDING, NONE);
		assertEquals(service_b.lockStatus(102L), NONE);

		events_a.next().assertEq(102L, WAITING, HOLDING);
		assertEquals(service_a.lockStatus(102L), HOLDING);

		// unlock 103
		service_c.unlock(103L).get(3, SECONDS);

		events_c.next().assertEq(103L, HOLDING, NONE);
		assertEquals(service_c.lockStatus(103L), NONE);
		assertEquals(service_c.lockStatus(102L), WAITING);

		events_a.next().assertEq(103L, WAITING, HOLDING);
		assertEquals(service_a.lockStatus(103L), HOLDING);

		// unlock a
		service_a.unlockAll().get(3, SECONDS);

		events_a.next().assertEq(102L, HOLDING, NONE);
		assertEquals(service_a.lockStatus(102L), NONE);

		events_a.next().assertEq(103L, HOLDING, NONE);
		assertEquals(service_a.lockStatus(103L), NONE);

		events_c.next().assertEq(102L, WAITING, HOLDING);
		assertEquals(service_c.lockStatus(102L), HOLDING);

		events_b.next().assertEq(103L, WAITING, HOLDING);
		assertEquals(service_b.lockStatus(103L), HOLDING);

		// unlock b
		service_b.unlockAll().get(3, SECONDS);

		events_b.next().assertEq(101L, HOLDING, NONE);
		assertEquals(service_b.lockStatus(101L), NONE);

		events_b.next().assertEq(103L, HOLDING, NONE);
		assertEquals(service_b.lockStatus(103L), NONE);

		events_c.next().assertEq(101L, WAITING, HOLDING);
		assertEquals(service_c.lockStatus(101L), HOLDING);

		// unlock c
		service_c.unlockAll().get(3, SECONDS);

		events_c.next().assertEq(101L, HOLDING, NONE);
		assertEquals(service_c.lockStatus(101L), NONE);

		events_c.next().assertEq(102L, HOLDING, NONE);
		assertEquals(service_c.lockStatus(102L), NONE);
	}

	public void tryLock() throws Exception {
		enableEvents();

		assertEquals(service_a.tryLock(101L).get(3, SECONDS), HOLDING);
		events_a.next().assertEq(101L, NONE, HOLDING);
		assertEquals(service_a.lockStatus(101L), HOLDING);
		assertEquals(service_a.tryLock(101L).get(3, SECONDS), HOLDING);

		assertEquals(service_b.tryLock(101L).get(3, SECONDS), NONE);
		assertNull(events_b.next(1));
		assertEquals(service_b.lockStatus(101L), NONE);

		assertEquals(service_c.lock(101L).get(3, SECONDS), WAITING);
		events_c.next().assertEq(101L, NONE, WAITING);
		assertEquals(service_c.lockStatus(101L), WAITING);
		assertEquals(service_c.tryLock(101L).get(3, SECONDS), WAITING);

		service_a.unlock(101L);
		events_a.next().assertEq(101L, HOLDING, NONE);
		events_c.next().assertEq(101L, WAITING, HOLDING);
		service_c.unlockAll();
		events_c.next().assertEq(101L, HOLDING, NONE);
		assertNull(events_b.next(1));
	}

	public void reset_by_disconnect() throws Exception {
		enableEvents();

		assertEquals(service_a.lock(101L).get(3, SECONDS), HOLDING);
		events_a.next().assertEq(101L, NONE, HOLDING);
		assertEquals(service_b.lock(101L).get(3, SECONDS), WAITING);
		events_b.next().assertEq(101L, NONE, WAITING);
		assertEquals(service_c.lock(101L).get(3, SECONDS), WAITING);
		events_c.next().assertEq(101L, NONE, WAITING);
		assertEquals(service_d.lock(101L).get(3, SECONDS), WAITING);
		events_d.next().assertEq(101L, NONE, WAITING);
		assertEquals(service_e.lock(101L).get(3, SECONDS), WAITING);
		events_e.next().assertEq(101L, NONE, WAITING);

		// disconnect the coordinator/leader/holder
		channel(0).disconnect(); // [B,C,D,E]
		// reset to [B,C,D,E], notified by reset command.
		events_a.next().assertEq(101L, HOLDING, NONE);
		events_b.next().assertEq(101L, WAITING, HOLDING);

		// disconnect a participant
		channel(2).disconnect(); // [B,D,E]
		// reset to [B,D,E], notified by reset command.
		events_c.next().assertEq(101L, WAITING, NONE);

		service_b.unlock(101L);
		events_b.next().assertEq(101L, HOLDING, NONE);
		events_d.next().assertEq(101L, WAITING, HOLDING);

		// disconnect the holder and lost majority
		channel(3).disconnect(); // [B,E]
		// no reset, notify base on local status.
		events_d.next().assertEq(101L, HOLDING, NONE);
		events_e.next().assertEq(101L, WAITING, NONE);

		// reconnect the previous holder and reach majority
		service_d = reconnect(channel(3), events_d); // [B,E,D]
		assertNull(events_d.next()); // D has a new address
		// reset to clear all previous status, notified by reset command.
		events_e.next().assertEq(101L, WAITING, NONE); // duplicated

		assertEquals(service_d.lock(101L).get(3, SECONDS), HOLDING);
		assertEquals(service_e.lock(101L).get(3, SECONDS), WAITING);
		events_d.next().assertEq(101L, NONE, HOLDING);
		events_e.next().assertEq(101L, NONE, WAITING);

		// disconnect to lost majority
		channel(1).disconnect(); // [E,D]
		// no reset, notify base on local status.
		events_d.next().assertEq(101L, HOLDING, NONE);
		events_e.next().assertEq(101L, WAITING, NONE);

		// reconnect to reach majority
		service_b = reconnect(channel(1), events_b); // [E,D,B]
		// reset to clear all previous status, notified by reset command.
		events_d.next().assertEq(101L, HOLDING, NONE); // duplicated
		events_e.next().assertEq(101L, WAITING, NONE); // duplicated
	}

	public void reset_by_partition() throws Exception {
		enableEvents();

		assertEquals(service_d.lock(101L).get(3, SECONDS), HOLDING);
		events_d.next().assertEq(101L, NONE, HOLDING);
		assertEquals(service_e.lock(102L).get(3, SECONDS), HOLDING);
		events_e.next().assertEq(102L, NONE, HOLDING);

		assertEquals(service_a.lock(101L).get(3, SECONDS), WAITING);
		assertEquals(service_a.lock(102L).get(3, SECONDS), WAITING);
		events_a.next().assertEq(101L, NONE, WAITING);
		events_a.next().assertEq(102L, NONE, WAITING);

		// partition into a majority subgroup and minority subgroup
		partition(new int[]{0, 1, 2}, new int[]{3, 4});

		events_d.next().assertEq(101L, HOLDING, NONE);
		events_e.next().assertEq(102L, HOLDING, NONE);

		events_a.next().assertEq(101L, WAITING, HOLDING);
		events_a.next().assertEq(102L, WAITING, HOLDING);

		merge(0, 3);

		events_d.next().assertEq(101L, HOLDING, NONE);
		events_e.next().assertEq(102L, HOLDING, NONE);

		assertEquals(service_b.lock(101L).get(3, SECONDS), WAITING);
		assertEquals(service_c.lock(102L).get(3, SECONDS), WAITING);
		events_b.next().assertEq(101L, NONE, WAITING);
		events_c.next().assertEq(102L, NONE, WAITING);

		assertEquals(service_a.lockStatus(101L), HOLDING);
		assertEquals(service_a.lockStatus(102L), HOLDING);

		// partition into subgroups without majority
		partition(new int[]{0, 1}, new int[]{2}, new int[]{3, 4});

		events_a.next().assertEq(101L, HOLDING, NONE);
		events_a.next().assertEq(102L, HOLDING, NONE);
		events_b.next().assertEq(101L, WAITING, NONE);
		events_c.next().assertEq(102L, WAITING, NONE);

		merge(0, 2, 3);
		waitUntilLeaderElected(0, 1, 2, 3, 4);

		events_a.next().assertEq(101L, HOLDING, NONE);
		events_a.next().assertEq(102L, HOLDING, NONE);
		events_b.next().assertEq(101L, WAITING, NONE);
		events_c.next().assertEq(102L, WAITING, NONE);

		assertEquals(service_a.lock(101L).get(3, SECONDS), HOLDING);
		events_a.next().assertEq(101L, NONE, HOLDING);
		assertEquals(service_e.lock(101L).get(3, SECONDS), WAITING);
		events_e.next().assertEq(101L, NONE, WAITING);

		channel(0).disconnect();

		events_a.next().assertEq(101L, HOLDING, NONE);
		events_e.next().assertEq(101L, WAITING, HOLDING);
	}

	public void snapshot() throws Exception {
		enableEvents();

		assertEquals(service_a.lock(101L).get(3, SECONDS), HOLDING);
		events_a.next().assertEq(101L, NONE, HOLDING);
		assertEquals(service_b.lock(101L).get(3, SECONDS), WAITING);
		events_b.next().assertEq(101L, NONE, WAITING);

		partition(new int[]{0, 2, 3, 4}, new int[]{1});

		events_b.next().assertEq(101, WAITING, NONE);

		service_a.unlock(101).get(3, SECONDS);
		assertEquals(service_c.lock(101).get(3, SECONDS), HOLDING);
		assertEquals(service_d.lock(101).get(3, SECONDS), WAITING);
		service_c.unlock(101).get(3, SECONDS);
		service_d.unlock(101).get(3, SECONDS);

		List<LockService> services = List.of(service_a, service_c, service_d, service_e);
		for (int i = 0; i < 100; i++) {
			long key = -(i + 1);
			for (int t = 0, len = services.size(); t < len; t++) {
				services.get((i + t) % len).lock(key).get(3, SECONDS);
			}
			services.get(i % services.size()).unlock(key).get(3, SECONDS);
		}

		service_c.unlockAll();

		leader().snapshotAsync().get(3, SECONDS);

		merge(0, 1);

		waitUntilNodesApplyAllLogs();
		assertTrue(events_b.queue.isEmpty());

		Map<Long, List<UUID>> state = service_a.dumpState();
		assertEquals(service_b.dumpState(), state);
		assertEquals(service_c.dumpState(), state);
		assertEquals(service_d.dumpState(), state);
		assertEquals(service_e.dumpState(), state);
	}

	public void mutex_atomicity() throws Exception {
		Lock a = service_a.mutex(101);
		Lock b = service_b.mutex(101);
		Lock c = service_c.mutex(101);
		Lock d = service_d.mutex(101);
		Lock e = service_e.mutex(101);

		class MutableInt {
			int value;
		}
		MutableInt count = new MutableInt();
		List<Thread> threads = Stream.of(a, b, c, d, e).flatMap(t -> Stream.of(t, t)).map(t -> new Thread(() -> {
			for (int i = 0; i < 100; i++) {
				t.lock();
				try {
					int v = count.value;
					LockSupport.parkNanos(10);
					count.value = v + 1;
				} finally {
					t.unlock();
				}
			}
		})).collect(toList());

		threads.forEach(Thread::start);
		for (Thread t : threads) t.join();

		assertEquals(count.value, 1000);
	}

	public void mutex_interruption() throws InterruptedException {
		Mutex a = service_a.mutex(101);
		Mutex b = service_b.mutex(101);
		a.lock();
		List<CompletableFuture<Void>> list;
		try {
			CompletableFuture.runAsync(() -> {
				interruptAfter(1);
				assertThrows(InterruptedException.class, a::lockInterruptibly);

				interruptAfter(1);
				assertThrows(InterruptedException.class, b::lockInterruptibly);

				interruptAfter(1);
				assertThrows(InterruptedException.class, () -> a.tryLock(30, SECONDS));

				interruptAfter(1);
				assertThrows(InterruptedException.class, () -> b.tryLock(30, SECONDS));
			}).join();

			BlockingQueue<CompletableFuture<Void>> interrupted = new LinkedBlockingQueue<>();
			list = Stream.of(a, b).map(t -> CompletableFuture.runAsync(() -> {
				interrupted.add(interruptAfter(1));
				t.lock();
				try {
					assertTrue(Thread.currentThread().isInterrupted());
				} finally {
					t.unlock();
				}
				assertTrue(Thread.currentThread().isInterrupted());
			})).collect(toList());
			for (int i = 0, l = list.size(); i < l; i++) interrupted.take().join();
		} finally {
			a.unlock();
		}
		list.forEach(CompletableFuture::join);
	}

	public void mutex_timeout() {
		Mutex a = service_a.mutex(101);
		Mutex b = service_b.mutex(101);
		a.lock();
		try {
			CompletableFuture.runAsync(() -> {
				try {
					long timeout = SECONDS.toNanos(1);
					Offset<Long> error = offset(MILLISECONDS.toNanos(100));

					long begin = System.nanoTime();
					assertFalse(a.tryLock(timeout, NANOSECONDS));
					assertThat(System.nanoTime() - begin).isCloseTo(timeout, error);

					begin = System.nanoTime();
					assertFalse(b.tryLock(timeout, NANOSECONDS));
					assertThat(System.nanoTime() - begin).isCloseTo(timeout, error);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}).join();
		} finally {
			a.unlock();
		}
	}

	public void mutex_race() {
		Mutex a = service_a.mutex(101);
		Mutex b = service_b.mutex(101);
		Mutex c = service_c.mutex(101);

		List.of(Stream.of(a, b), Stream.of(b, c)).forEach(stream -> {
			stream.map(t -> CompletableFuture.runAsync(() -> {
				for (int i = 0; i < 3000; i++) {
					t.lock(); t.unlock();
				}
			})).collect(toList()).forEach(t -> {
				try {
					t.get(10, SECONDS);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		});
	}

	public void mutex_inconsistency() throws Exception {
		Mutex a = service_a.mutex(101);
		Mutex b = service_b.mutex(101);

		for (Mutex t : List.of(a, b)) {
			CompletableFuture<Mutex> unlocked = new CompletableFuture<>();
			CompletableFuture<Mutex> locked = new CompletableFuture<>();
			t.setUnexpectedUnlockHandler(unlocked::complete);
			t.setUnexpectedLockHandler(locked::complete);

			t.lock();
			try {
				t.service().unlock(101).join();
				assertSame(unlocked.get(3, SECONDS), t);
				assertSame(t.getHolder(), Thread.currentThread());
				assertEquals(t.getStatus(), NONE);
			} finally {
				t.unlock();
			}

			t.service().lock(101).join();
			assertSame(locked.get(3, SECONDS), t);
			assertEquals(t.getStatus(), HOLDING);

			if (t.tryLock()) t.unlock();
			assertEquals(t.getStatus(), NONE);
		}

		a.lock(); CompletableFuture<Void> f;
		try {
			f = CompletableFuture.runAsync(() -> {
				b.lock();
				try {
					assertEquals(service_b.lockStatus(101), HOLDING);
				} finally {
					b.unlock();
				}
			});
			Util.waitUntil(5000, 1000, () -> service_b.lockStatus(101) == WAITING);
			service_b.unlock(101).get(3, SECONDS);
			waitUntilNodesApplyAllLogs();
			Util.waitUntil(5000, 1000, () -> service_b.lockStatus(101) == WAITING);
		} finally {
			a.unlock();
		}
		f.get(5, SECONDS);
	}

	public void mutex_exception() {
		Mutex a = service_a.mutex(101);

		service_a.raft.throwingInterceptor(new Exception("thrown error"));
		assertThatThrownBy(a::lock).isInstanceOf(LockService.RaftException.class)
				.cause().isInstanceOf(Exception.class).hasMessage("thrown error");

		service_a.raft.errorInterceptor(new Exception("returned error"));
		assertThatThrownBy(a::lock).isInstanceOf(LockService.RaftException.class)
				.cause().isInstanceOf(Exception.class).hasMessage("returned error");

		service_a.raft.voidInterceptor();
		a.setTimeout(1000);
		assertThatThrownBy(a::lock).isInstanceOf(LockService.RaftException.class)
				.cause().isInstanceOf(TimeoutException.class);

		service_a.raft.removeInterceptor();
		a.lock();
		try {
			service_a.raft.throwingInterceptor(new Exception("thrown error"));
		} finally {
			assertThatThrownBy(a::unlock).isInstanceOf(LockService.RaftException.class)
					.cause().isInstanceOf(Exception.class).hasMessage("thrown error");
		}

		a.lock();
		service_a.raft.removeInterceptor();
		a.unlock();
	}

	private CompletableFuture<Void> interruptAfter(int delay) {
		CompletableFuture<Void> done = new CompletableFuture<>();
		Thread thread = Thread.currentThread();
		CompletableFuture.delayedExecutor(delay, SECONDS).execute(() -> {
			thread.interrupt(); done.complete(null);
		});
		return done;
	}

	private Service reconnect(JChannel ch, LockService.Listener listener) throws Exception {
		Service service = new Service(ch);
		if (listener != null) service.addListener(listener);
		ch.connect(clusterName()); return service;
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

	private void waitUntilNodesApplyAllLogs(int... indexes) throws TimeoutException {
		RAFT[] rafts = indexes.length > 0 ? IntStream.of(indexes).mapToObj(this::raft).toArray(RAFT[]::new) :
				stream(channels()).map(this::raft).toArray(RAFT[]::new);
		Util.waitUntil(30_000, 1000, () -> {
			long last = -1;
			for (RAFT raft : rafts) {
				if (last == -1) last = raft.lastAppended();
				else if (raft.lastAppended() != last) return false;
				if (raft.commitIndex() != last) return false;
			}
			return true;
		});
	}

	private void waitUntilLeaderElected(int... indexes) {
		RAFT[] rafts = IntStream.of(indexes).mapToObj(this::raft).toArray(RAFT[]::new);
		BaseRaftElectionTest.waitUntilLeaderElected(rafts, 10_000);
	}
}
