package org.jgroups.protocols.raft.internal.snapshot;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.raft.StateMachine;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SynchronousSnapshotManagerTest {

    private static final String LOG_NAME = "ssm-test-log";
    private static final List<String> MEMBERS = List.of("A", "B", "C");

    private InMemoryLog log;
    private PersistentState persistentState;
    private DefaultSnapshotMetrics metrics;

    @BeforeMethod
    protected void setup() throws Exception {
        log = new InMemoryLog();
        log.init(LOG_NAME, null);
        persistentState = new PersistentState();
        persistentState.setMembers(MEMBERS);
        metrics = new DefaultSnapshotMetrics();
    }

    @AfterMethod
    protected void cleanup() {
        InMemoryLog.logs.remove(LOG_NAME);
    }

    public void testCreate() throws Exception {
        CounterStateMachine sm = new CounterStateMachine(42);
        SynchronousSnapshotManager manager = createManager(sm, noOpSender());

        manager.create(5, idx -> assertThat(idx).isEqualTo(5));

        assertThat(metrics.numSnapshots()).isEqualTo(1);

        ByteBuffer snapshot = log.getSnapshot();
        assertThat(snapshot).isNotNull();

        DataInput in = new ByteArrayDataInputStream(snapshot);
        PersistentState restored = new PersistentState();
        restored.readFrom(in);
        assertThat(restored.getMembers()).containsExactlyInAnyOrderElementsOf(MEMBERS);
        assertThat(in.readInt()).isEqualTo(42);
    }

    public void testCreateWithNullStateMachine() {
        SynchronousSnapshotManager manager = createManager(null, noOpSender());

        assertThatThrownBy(() -> manager.create(1, idx -> {}))
                .isInstanceOf(IllegalStateException.class);
        assertThat(metrics.numSnapshots()).isZero();
    }

    public void testCreateFailure() {
        StateMachine failing = new FailingStateMachine(true, false);
        SynchronousSnapshotManager manager = createManager(failing, noOpSender());

        AtomicBoolean actionCalled = new AtomicBoolean();
        assertThatThrownBy(() -> manager.create(1, idx -> actionCalled.set(true)))
                .isInstanceOf(IOException.class);

        assertThat(actionCalled.get()).isFalse();
        assertThat(metrics.numSnapshots()).isZero();
        assertThat(metrics.numFailedSnapshotsTaken()).isEqualTo(1);
    }

    public void testInstall() throws Exception {
        CounterStateMachine original = new CounterStateMachine(42);
        ByteBuffer data = createSnapshotBuffer(persistentState, original);

        CounterStateMachine target = new CounterStateMachine(0);
        PersistentState targetState = new PersistentState();
        SynchronousSnapshotManager manager = new SynchronousSnapshotManager(
                target, targetState, log, noOpSender(), metrics);

        manager.install(data, 5, 2, (idx, term) -> {
            assertThat(idx).isEqualTo(5);
            assertThat(term).isEqualTo(2);
        });

        assertThat(target.counter).isEqualTo(original.counter);
        assertThat(targetState.getMembers()).containsExactlyInAnyOrderElementsOf(MEMBERS);

        assertThat(log.firstAppended()).isEqualTo(5);
        assertThat(log.lastAppended()).isEqualTo(5);
        assertThat(log.commitIndex()).isEqualTo(5);

        assertThat(log.getSnapshot()).isNotNull();
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);
    }

    public void testInstallFailure() throws Exception {
        CounterStateMachine original = new CounterStateMachine(42);
        ByteBuffer data = createSnapshotBuffer(persistentState, original);

        StateMachine failing = new FailingStateMachine(false, true);
        SynchronousSnapshotManager manager = createManager(failing, noOpSender());

        AtomicBoolean actionCalled = new AtomicBoolean();
        assertThatThrownBy(() -> manager.install(data, 5, 2, (idx, term) -> actionCalled.set(true)))
                .isInstanceOf(RuntimeException.class);

        assertThat(actionCalled.get()).isFalse();
        assertThat(metrics.numSnapshotsReceived()).isZero();
        assertThat(metrics.numFailedSnapshotsInstalled()).isEqualTo(1);
    }

    public void testTransferTo() throws Exception {
        CounterStateMachine sm = new CounterStateMachine(42);
        ByteBuffer snapshot = createSnapshotBuffer(persistentState, sm);
        log.setSnapshot(snapshot);

        Address dest = Util.createRandomAddress("target");
        AtomicBoolean senderCalled = new AtomicBoolean();

        SnapshotSender capturingSender = (d, sn, idx, term) -> {
            assertThat(d).isEqualTo(dest);
            assertThat(idx).isEqualTo(5);
            assertThat(term).isEqualTo(2);
            assertThat(sn).isNotNull();
            assertThat(sn.remaining()).isGreaterThan(0);
            senderCalled.set(true);
        };

        SynchronousSnapshotManager manager = createManager(sm, capturingSender);
        manager.transferTo(dest, 5, 2);

        assertThat(senderCalled.get()).isTrue();
    }

    public void testMetricsReset() throws Exception {
        CounterStateMachine sm = new CounterStateMachine(42);
        SynchronousSnapshotManager manager = createManager(sm, noOpSender());

        manager.create(1, idx -> {});

        ByteBuffer data = createSnapshotBuffer(persistentState, sm);
        manager.install(data, 1, 1, (idx, term) -> {});

        assertThat(metrics.numSnapshots()).isEqualTo(1);
        assertThat(metrics.numSnapshotsReceived()).isEqualTo(1);

        metrics.reset();

        assertThat(metrics.numSnapshots()).isZero();
        assertThat(metrics.numSnapshotsReceived()).isZero();
        assertThat(metrics.numFailedSnapshotsTaken()).isZero();
        assertThat(metrics.numFailedSnapshotsInstalled()).isZero();
    }

    private SynchronousSnapshotManager createManager(StateMachine sm, SnapshotSender sender) {
        return new SynchronousSnapshotManager(sm, persistentState, log, sender, metrics);
    }

    private static SnapshotSender noOpSender() {
        return (dest, snapshot, lastIndex, lastTerm) -> {};
    }

    private static ByteBuffer createSnapshotBuffer(PersistentState ps, StateMachine sm) throws Exception {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(256, true);
        ps.writeTo(out);
        sm.writeContentTo(out);
        return ByteBuffer.wrap(out.buffer(), 0, out.position());
    }

    private static class CounterStateMachine implements StateMachine {
        int counter;

        CounterStateMachine(int initial) {
            this.counter = initial;
        }

        @Override
        public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
            return null;
        }

        @Override
        public void readContentFrom(DataInput in) {
            try {
                counter = in.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void writeContentTo(DataOutput out) throws Exception {
            out.writeInt(counter);
        }
    }

    private static class FailingStateMachine implements StateMachine {
        private final boolean failOnWrite;
        private final boolean failOnRead;

        FailingStateMachine(boolean failOnWrite, boolean failOnRead) {
            this.failOnWrite = failOnWrite;
            this.failOnRead = failOnRead;
        }

        @Override
        public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
            return null;
        }

        @Override
        public void readContentFrom(DataInput in) {
            if (failOnRead) throw new RuntimeException("simulated read failure");
        }

        @Override
        public void writeContentTo(DataOutput out) throws Exception {
            if (failOnWrite) throw new IOException("simulated write failure");
        }
    }
}
