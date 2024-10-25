package org.jgroups.raft.blocks;

import static org.jgroups.raft.blocks.LockService.LockStatus.HOLDING;
import static org.jgroups.raft.blocks.LockService.LockStatus.NONE;
import static org.jgroups.raft.blocks.LockService.LockStatus.WAITING;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.jgroups.Address;
import org.jgroups.ChannelListener;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.UUID;

/**
 * @author Zhang Yifei
 */
public class LockService {
	protected static final Log log = LogFactory.getLog(LockService.class);

	protected static final byte LOCK = 1, TRY_LOCK = 2, UNLOCK = 3, UNLOCK_ALL = 4, RESET = 5;

	protected final RaftHandle raft;
	protected final Map<Long, LockEntry> locks = new HashMap<>();
	protected final Map<UUID, Set<LockEntry>> memberLocks = new LinkedHashMap<>();

	protected volatile View view;
	protected ExtendedUUID address;

	protected final ConcurrentMap<Long, LockStatus> lockStatus = new ConcurrentHashMap<>();
	protected final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
	protected final ConcurrentMap<Long, Mutex> mutexes = new ConcurrentHashMap<>(); // could be weak value reference

	public LockService(JChannel channel) {
		if (channel.isConnecting() || channel.isConnected()) {
			throw new IllegalStateException("Illegal channel state " + channel.getState());
		}
		Hook hook = createHook();
		raft = createRaft(channel, hook);
		channel.setUpHandler(hook).addChannelListener(hook);
		raft.addRoleListener(hook);
	}

	protected Hook createHook() {
		return new Hook();
	}

	protected RaftHandle createRaft(JChannel ch, StateMachine sm) {
		return new RaftHandle(ch, sm);
	}

	protected static class LockEntry {
		public final long id;
		public final LinkedHashSet<UUID> waiters = new LinkedHashSet<>();
		public UUID holder;

		protected LockEntry(long id) {this.id = id;}

		protected UUID unlock() {
			// make sure it's a consistent result for all nodes
			var i = waiters.iterator();
			if (!i.hasNext()) return holder = null;
			var v = i.next(); i.remove(); return holder = v;
		}
	}

	protected class Hook implements StateMachine, RAFT.RoleChange, UpHandler, ChannelListener {

		@Override
		public void readContentFrom(DataInput in) {
			Map<Long, LockStatus> tmp = new HashMap<>();
			locks.clear(); memberLocks.clear();
			for (int i = 0, l = readInt(in); i < l; i++) {
				long id = readLong(in);
				LockEntry lock = new LockEntry(id);
				lock.holder = readUuid(in);
				for (int t = 0, m = readInt(in); t < m; t++) {
					lock.waiters.add(readUuid(in));
				}
				locks.put(id, lock);
				bind(lock.holder, lock);
				if (address.equals(lock.holder)) tmp.put(lock.id, HOLDING);
				for (var waiter : lock.waiters) {
					bind(waiter, lock);
					if (address.equals(waiter)) tmp.put(lock.id, WAITING);
				}
			}

			// notify base on local status
			lockStatus.forEach((k, v) -> notifyListeners(k, v, tmp.remove(k), false));
			tmp.forEach((k, v) -> notifyListeners(k, NONE, v, false));
		}

		@Override
		public void writeContentTo(DataOutput out) {
			writeInt((int) locks.values().stream().filter(t -> t.holder != null).count(), out);
			for (var lock : locks.values()) {
				if (lock.holder == null) continue;
				writeLong(lock.id, out);
				writeUuid(lock.holder, out);
				writeInt(lock.waiters.size(), out);
				for (UUID t : lock.waiters) {
					writeUuid(t, out);
				}
			}
		}

		@Override
		public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
			var in = new ByteArrayDataInputStream(data, offset, length);
			LockStatus status = null;
			switch (in.readByte()) {
				case LOCK:
					status = doLock(readLong(in), readUuid(in), false);
					break;
				case TRY_LOCK:
					status = doLock(readLong(in), readUuid(in), true);
					break;
				case UNLOCK:
					LockEntry lock = locks.computeIfAbsent(readLong(in), LockEntry::new);
					doUnlock(readUuid(in), lock, null);
					break;
				case UNLOCK_ALL:
					doUnlock(readUuid(in), null);
					break;
				case RESET:
					int len = readInt(in);
					List<UUID> members = new ArrayList<>(len);
					for (int i = 0; i < len; i++) {
						members.add(readUuid(in));
					}
					doReset(members);
					break;
			}
			return serialize_response && status != null ? new byte[] {(byte) status.ordinal()} : null;
		}

		@Override
		public void roleChanged(Role role) {
			if (role == Role.Leader) {
				try {
					// Reset after the leader is elected
					View v = view;
					boolean clear = false;
					if (v instanceof MergeView) {
						int majority = raft.raft().majority();
						clear = ((MergeView) v).getSubgroups().stream().allMatch(t -> t.size() < majority);
					}
					reset(clear ? null : v);
				} catch (Throwable e) {
					log.error("Fail to send reset command", e);
				}
			}
		}

		@Override
		public UpHandler setLocalAddress(Address a) {
			address = (ExtendedUUID) a; return this;
		}

		@Override
		public Object up(Event evt) {
			if (evt.getType() == Event.VIEW_CHANGE) {
				handleView(evt.arg());
			}
			return null;
		}

		@Override
		public Object up(Message msg) {return null;}

		@Override
		public void channelDisconnected(JChannel channel) {
			cleanup();
		}
	}

	protected LockStatus doLock(long lockId, UUID member, boolean trying) {
		LockEntry lock = locks.computeIfAbsent(lockId, LockEntry::new);
		LockStatus prev = NONE, next = HOLDING;
		if (lock.holder == null) {
			lock.holder = member;
		} else if (lock.holder.equals(member)) {
			prev = HOLDING;
		} else if (trying) {
			prev = next = lock.waiters.contains(member) ? WAITING : NONE;
		} else {
			if (!lock.waiters.add(member)) prev = WAITING;
			next = WAITING;
		}
		if (prev != next) bind(member, lock);
		if (address.equals(member)) {
			notifyListeners(lockId, prev, next, false);
		}
		if (log.isTraceEnabled()) {
			log.trace("[%s] %s lock %s, prev: %s, next: %s", address, member, lockId, prev, next);
		}
		return next;
	}

	protected void doUnlock(UUID member, Set<UUID> unlocking) {
		Set<LockEntry> set = memberLocks.get(member); if (set == null) return;
		for (LockEntry lock : set.toArray(LockEntry[]::new)) {
			doUnlock(member, lock, unlocking);
		}
	}

	protected void doUnlock(UUID member, LockEntry lock, Set<UUID> unlocking) {
		LockStatus prev = HOLDING;
		UUID holder = null;
		List<UUID> waiters = null;
		if (member.equals(lock.holder)) {
			do {
				if (holder != null) {
					if (waiters == null) waiters = new ArrayList<>(unlocking.size());
					waiters.add(holder);
				}
				holder = lock.unlock();
			} while (holder != null && unlocking != null && unlocking.contains(holder));
		} else {
			prev = lock.waiters.remove(member) ? WAITING : NONE;
		}
		if (prev != NONE) unbind(member, lock);
		if (address.equals(member)) {
			notifyListeners(lock.id, prev, NONE, false);
		} else if (address.equals(holder)) {
			notifyListeners(lock.id, WAITING, HOLDING, false);
		}
		if (log.isTraceEnabled()) {
			log.trace("[%s] %s unlock %s, prev: %s", address, member, lock.id, prev);
			if (holder != null)
				log.trace("[%s] %s lock %s, prev: %s, next: %s", address, holder, lock.id, WAITING, HOLDING);
		}
		if (waiters != null) for (UUID waiter : waiters) {
			unbind(waiter, lock);
			if (address.equals(waiter)) {
				notifyListeners(lock.id, WAITING, NONE, false);
			}
			if (log.isTraceEnabled()) {
				log.trace("[%s] %s unlock %s, prev: %s", address, waiter, lock.id, WAITING);
			}
		}
	}

	protected void doReset(List<UUID> members) {
		Set<UUID> prev = new LinkedHashSet<>(memberLocks.keySet());
		if (log.isTraceEnabled()) {
			log.trace("[%s] reset %s to %s", address, prev, members);
		}
		for (var id : members) prev.remove(id);
		for (var id : prev) doUnlock(id, prev);
	}

	protected void bind(UUID member, LockEntry lock) {
		memberLocks.computeIfAbsent(member, k -> new LinkedHashSet<>()).add(lock);
	}

	protected void unbind(UUID member, LockEntry lock) {
		memberLocks.computeIfPresent(member, (k, v) -> {
			v.remove(lock); return v.isEmpty() ? null : v;
		});
	}

	protected void notifyListeners(long lockId, LockStatus prev, LockStatus curr, boolean force) {
		if (!force && raft.leader() == null) return;
		if (prev == null) prev = NONE;
		if (curr == null) curr = NONE;
		LockStatus local = curr == NONE ? lockStatus.remove(lockId) : lockStatus.put(lockId, curr);
		if (prev == curr) {
			prev = local == null ? NONE : local;
			if (prev == curr) return;
		}
		Mutex mutex = mutexes.get(lockId);
		if (mutex != null) mutex.onStatusChange(lockId, prev, curr);
		for (Listener listener : listeners) {
			try {
				listener.onStatusChange(lockId, prev, curr);
			} catch (Throwable e) {
				log.error("Fail to notify listener, lock: %s, prev: %s, curr: %s", lockId, prev, curr, e);
			}
		}
	}

	protected void handleView(View next) {
		View prev = this.view; this.view = next;
		if (log.isTraceEnabled()) {
			log.trace("[%s] View accepted: %s, prev: %s, leader: %s", address, next, prev, raft.leader());
		}

		if (prev != null) {
			int majority = raft.raft().majority();
			if (prev.size() >= majority && next.size() < majority) { // lost majority
				// In partition case if majority is still working, it will be forced to unlock by reset command.
				cleanup();
			} else if (!next.containsMembers(prev.getMembersRaw()) && raft.isLeader()) { // member left
				try {
					reset(next);
				} catch (Throwable e) {
					log.error("Fail to send reset command", e);
				}
			}
		}
	}

	protected void cleanup() {
		lockStatus.forEach((k, v) -> notifyListeners(k, v, NONE, true));
	}

	protected void reset(View view) {
		Address[] members = view != null ? view.getMembersRaw() : new Address[0];
		int len = members.length;
		var out = new ByteArrayDataOutputStream(6 + len * 16);
		out.writeByte(RESET);
		writeInt(len, out);
		for (Address member : members) {
			writeUuid((UUID) member, out);
		}
		assert out.position() <= 6 + len * 16;
		invoke(out).thenApply(t -> null).exceptionally(e -> {
			log.error("Fail to reset to " + view, e); return null;
		});
	}

	/**
	 * Add listener
	 * @param listener listener for the status change.
	 * @return true if added, otherwise false.
	 */
	public boolean addListener(Listener listener) { return listeners.addIfAbsent(listener); }

	/**
	 * Remove listener
	 * @param listener listener for removing
	 * @return true if removed, otherwise false.
	 */
	public boolean removeListener(Listener listener) { return listeners.remove(listener); }

	/**
	 * Get this member's lock status from local state.
	 * @param lockId the lock's id
	 * @return lock status
	 */
	public LockStatus lockStatus(long lockId) {
		var v = lockStatus.get(lockId); return v == null ? NONE : v;
	}

	/**
	 * Acquire the lock, will join the waiting queue if the lock is held by another member currently.
	 * @param lockId the lock's id
	 * @return HOLDING if hold the lock, WAITING if in the waiting queue.
	 */
	public CompletableFuture<LockStatus> lock(long lockId) {
		var out = new ByteArrayDataOutputStream(26);
		out.writeByte(LOCK);
		writeLong(lockId, out);
		writeUuid(address(), out);
		assert out.position() <= 26;
		return invoke(out).thenApply(t -> LockStatus.values()[t[0]]);
	}

	/**
	 * Try to acquire the lock, won't join the waiting queue.
	 * @param lockId the lock's id
	 * @return HOLDING if hold the lock, NONE if the lock is held by another member.
	 */
	public CompletableFuture<LockStatus> tryLock(long lockId) {
		var out = new ByteArrayDataOutputStream(26);
		out.writeByte(TRY_LOCK);
		writeLong(lockId, out);
		writeUuid(address(), out);
		assert out.position() <= 26;
		return invoke(out).thenApply(t -> LockStatus.values()[t[0]]);
	}

	/**
	 * Release the lock if it's the holder, and take next waiting member from the queue to be the new holder if there
	 * is one. Remove from waiting queue if it's waiting. Do nothing if neither of them.
	 * @param lockId the lock's id
	 * @return async completion
	 */
	public CompletableFuture<Void> unlock(long lockId) {
		var out = new ByteArrayDataOutputStream(26);
		out.writeByte(UNLOCK);
		writeLong(lockId, out);
		writeUuid(address(), out);
		assert out.position() <= 26;
		return invoke(out).thenApply(t -> null);
	}

	/**
	 * Release all related locks for this member.
	 * @return async completion
	 */
	public CompletableFuture<Void> unlock() {
		var out = new ByteArrayDataOutputStream(17);
		out.writeByte(UNLOCK_ALL);
		writeUuid(address(), out);
		assert out.position() <= 17;
		return invoke(out).thenApply(t -> null);
	}

	protected UUID address() {
		return Objects.requireNonNull(address);
	}

	protected CompletableFuture<byte[]> invoke(ByteArrayDataOutputStream out) {
		try {
			return raft.setAsync(out.buffer(), 0, out.position());
		} catch (Throwable e) {
			throw new RaftException("Fail to execute command", e);
		}
	}

	/**
	 * Get the mutex for the specified id.
	 * @param lockId the id related to the mutex
	 * @return mutex instance
	 */
	public Mutex mutex(long lockId) {
		return mutexes.computeIfAbsent(lockId, Mutex::new);
	}

	/**
	 * The member's lock status
	 */
	public enum LockStatus {
		HOLDING, WAITING, NONE
	}

	/**
	 * Listen on the lock status changes
	 */
	public interface Listener {
		void onStatusChange(long key, LockStatus prev, LockStatus curr);
	}

	/**
	 * Exception for the raft cluster errors
	 */
	public static class RaftException extends RuntimeException {
		public RaftException(String message) { super(message); }
		public RaftException(Throwable cause) { super(cause); }
		public RaftException(String message, Throwable cause) { super(message, cause); }
	}

	/**
	 * A distributed lock that backed on the lock service.
	 */
	public class Mutex implements Lock {
		private final long key;
		private volatile LockStatus status = NONE;
		private volatile Thread holder;
		private final AtomicInteger acquirers = new AtomicInteger();
		private final ReentrantLock delegate = new ReentrantLock();
		private final Condition notWaiting = delegate.newCondition();
		private Consumer<Mutex> lockHandler, unlockHandler;
		private long timeout = 8000;

		Mutex(long key) {this.key = key;}

		/**
		 * Set the timeout for the command executing in the lock service.
		 * @param timeout in milliseconds
		 */
		public void setTimeout(long timeout) {this.timeout = timeout;}

		/**
		 * The lock status in the lock service
		 * @return lock status of the key
		 */
		public LockStatus getStatus() {return status;}

		/**
		 * The current holder of this mutex
		 * @return the thread which holding this mutex
		 */
		public Thread getHolder() {return holder;}

		/**
		 * Register a handler for the unexpected unlocking in the lock service.
		 * @param handler callback with this mutex
		 */
		public void setUnexpectedUnlockHandler(Consumer<Mutex> handler) {unlockHandler = handler;}

		/**
		 * Register a handler for the unexpected locking in the lock service.
		 * @param handler callback with this mutex
		 */
		public void setUnexpectedLockHandler(Consumer<Mutex> handler) {lockHandler = handler;}

		/**
		 * Get the lock service
		 * @return the underlying lock service
		 */
		public LockService service() {return LockService.this;}

		/**
		 * @throws RaftException if exception happens during sending or executing commands in the lock service.
		 */
		@Override
		public void lock() {
			delegate.lock();
			acquirers.incrementAndGet();
			while (status != HOLDING) {
				try {
					if (status == WAITING) notWaiting.awaitUninterruptibly();
					else status = join(LockService.this.lock(key));
				} catch (Throwable e) {
					rethrow(unlock(e));
				}
			}
			holder = Thread.currentThread();
		}

		/**
		 * @throws RaftException if exception happens during sending or executing commands in the lock service.
		 */
		@Override
		public void lockInterruptibly() throws InterruptedException {
			delegate.lockInterruptibly();
			acquirers.incrementAndGet();
			while (status != HOLDING) {
				try {
					if (status == WAITING) notWaiting.await();
					else status = join(LockService.this.lock(key));
				} catch (InterruptedException e) {
					throw unlock(e);
				} catch (Throwable e) {
					rethrow(unlock(e));
				}
			}
			holder = Thread.currentThread();
		}

		/**
		 * @throws RaftException if exception happens during sending or executing commands in the lock service.
		 */
		@Override
		public boolean tryLock() {
			if (!delegate.tryLock()) return false;
			acquirers.incrementAndGet();
			if (status == NONE) {
				try {
					status = join(LockService.this.tryLock(key));
				} catch (Throwable ignored) {
				}
			}
			if (status == HOLDING) {
				holder = Thread.currentThread(); return true;
			}
			unlock(); return false;
		}

		/**
		 * @throws RaftException if exception happens during sending or executing commands in the lock service.
		 */
		@Override
		public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
			long deadline = System.nanoTime() + unit.toNanos(timeout), ns;
			if (!delegate.tryLock(timeout, unit)) return false;
			acquirers.incrementAndGet();
			while (status != HOLDING && (ns = deadline - System.nanoTime()) > 0) {
				try {
					if (status == WAITING) notWaiting.awaitNanos(ns);
					else status = join(LockService.this.lock(key));
				} catch (InterruptedException e) {
					throw unlock(e);
				} catch (Throwable e) {
					rethrow(unlock(e));
				}
			}
			if (status == HOLDING) {
				holder = Thread.currentThread(); return true;
			}
			unlock(); return false;
		}

		/**
		 * @throws RaftException if exception happens during sending or executing commands in the lock service.
		 */
		@Override
		public void unlock() {
			if (!delegate.isHeldByCurrentThread()) return;
			assert holder == null || holder == Thread.currentThread();
			if (delegate.getHoldCount() == 1) holder = null;
			try {
				if (acquirers.decrementAndGet() == 0 && status != NONE) {
					join(LockService.this.unlock(key));
					status = NONE;
				}
			} catch (Throwable e) {
				rethrow(e);
			} finally {
				delegate.unlock();
			}
		}

		/**
		 * Unsupported
		 */
		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException();
		}

		private <T extends Throwable> T unlock(T error) {
			try {
				unlock();
			} catch (Throwable e) {
				error.addSuppressed(e);
			}
			return error;
		}

		private <T> T join(CompletableFuture<T> future) throws ExecutionException, TimeoutException {
			long nanos = TimeUnit.MILLISECONDS.toNanos(timeout), deadline = System.nanoTime() + nanos;
			boolean interrupted = Thread.interrupted();
			try {
				do {
					try {
						return future.get(nanos, TimeUnit.NANOSECONDS);
					} catch (InterruptedException e) {
						interrupted = true;
					}
				} while ((nanos = deadline - System.nanoTime()) > 0);
				throw new TimeoutException();
			} finally {
				if (interrupted) Thread.currentThread().interrupt();
			}
		}

		void onStatusChange(long key, LockStatus prev, LockStatus curr) {
			if (key != this.key) return;
			if (curr != HOLDING && holder != null) {
				status = curr;
				var handler = unlockHandler;
				if (handler != null) try {
					handler.accept(this);
				} catch (Throwable e) {
					log.error("Error occurred on unlock handler", e);
				}
			} else if (curr != NONE && acquirers.get() == 0) {
				status = curr;
				var handler = lockHandler;
				if (handler != null) try {
					handler.accept(this);
				} catch (Throwable e) {
					log.error("Error occurred on lock handler", e);
				}
			} else if (prev == WAITING) {
				delegate.lock();
				try {
					if (status == WAITING) {
						status = curr;
						notWaiting.signalAll();
					}
				} finally {
					delegate.unlock();
				}
			}
		}
	}

	private static <T> T rethrow(Throwable e) {
		if (e instanceof RaftException) throw (RaftException) e;
		if (e instanceof CompletionException) {
			Throwable cause = e.getCause();
			throw cause != null ? new RaftException(e) : (CompletionException) e;
		}
		if (e instanceof ExecutionException) throw new RaftException(e.getCause());
		if (e instanceof TimeoutException) throw new RaftException("Execute command timeout", e);
		throw new RaftException("Unknown exception", e);
	}

	private static void writeInt(int value, DataOutput out) {
		try {
			for (; (value & ~0x7F) != 0; value >>>= 7) {
				out.writeByte(0x80 | (value & 0x7F));
			}
			out.writeByte(value);
		} catch (IOException e) {
			throw new RaftException("Fail to write", e);
		}
	}

	private static int readInt(DataInput in) {
		try {
			int v = in.readByte(); if (v >= 0) return v;
			if ((v ^= in.readByte() << 7)  <  0) return v ^ 0xFFFFFF80;
			if ((v ^= in.readByte() << 14) >= 0) return v ^ 0x00003F80;
			if ((v ^= in.readByte() << 21) <  0) return v ^ 0xFFE03F80;
			return v ^ in.readByte() << 28 ^ 0x0FE03F80;
		} catch (IOException e) {
			throw new RaftException("Fail to read", e);
		}
	}

	private static void writeLong(long value, DataOutput out) {
		try {
			for (int i = 0; i < 8 && (value & ~0x7FL) != 0; i++) {
				out.writeByte(0x80 | ((int) value & 0x7F));
				value >>>= 7;
			}
			out.writeByte((int) value);
		} catch (IOException e) {
			throw new RaftException("Fail to write", e);
		}
	}

	private static long readLong(DataInput in) {
		try {
			long v = in.readByte(); if (v >= 0) return v;
			if ((v ^= (long) in.readByte() << 7)  <  0L) return v ^ 0xFFFFFFFFFFFFFF80L;
			if ((v ^= (long) in.readByte() << 14) >= 0L) return v ^ 0x0000000000003F80L;
			if ((v ^= (long) in.readByte() << 21) <  0L) return v ^ 0xFFFFFFFFFFE03F80L;
			if ((v ^= (long) in.readByte() << 28) >= 0L) return v ^ 0x000000000FE03F80L;
			if ((v ^= (long) in.readByte() << 35) <  0L) return v ^ 0xFFFFFFF80FE03F80L;
			if ((v ^= (long) in.readByte() << 42) >= 0L) return v ^ 0x000003F80FE03F80L;
			if ((v ^= (long) in.readByte() << 49) <  0L) return v ^ 0xFFFE03F80FE03F80L;
			return v ^ (long) in.readByte() << 56 ^ 0x00FE03F80FE03F80L;
		} catch (IOException e) {
			throw new RaftException("Fail to read", e);
		}
	}

	private static void writeUuid(UUID id, DataOutput out) {
		try {
			out.writeLong(id.getMostSignificantBits());
			out.writeLong(id.getLeastSignificantBits());
		} catch (IOException e) {
			throw new RaftException("Fail to write", e);
		}
	}

	private static UUID readUuid(DataInput in) {
		try {
			return new UUID(in.readLong(), in.readLong());
		} catch (IOException e) {
			throw new RaftException("Fail to read", e);
		}
	}
}
