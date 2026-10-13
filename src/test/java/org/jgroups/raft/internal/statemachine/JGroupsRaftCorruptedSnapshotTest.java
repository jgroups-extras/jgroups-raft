package org.jgroups.raft.internal.statemachine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftRole;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.filelog.SnapshotStorage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies a node restarting from a snapshot on disk either restores it completely or refuses to start.
 *
 * <p>
 * Starting with an empty state machine after failing to read the snapshot would replay only the entries after the
 * snapshot, leaving the node with state that silently diverges from the cluster.
 * </p>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftCorruptedSnapshotTest {

    private Path logDir;

    @BeforeMethod
    public void setUp() throws IOException {
        logDir = Files.createTempDirectory("corrupted-snapshot-test");
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (logDir != null && logDir.toFile().exists()) {
            try (Stream<Path> walk = Files.walk(logDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testRestartRestoresStateFromSnapshot() throws Exception {
        writeAndSnapshot(10);

        JGroupsRaft<Counter> restarted = buildRaft(new CounterImpl());
        try {
            restarted.start();
            assertThat(eventually(() -> restarted.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();
            assertThat(restarted.read(Counter::get)).isEqualTo(10);
        } finally {
            restarted.stop();
        }
    }

    public void testRestartWithCorruptedSnapshotFails() throws Exception {
        writeAndSnapshot(10);
        corruptSnapshotData();

        JGroupsRaft<Counter> restarted = buildRaft(new CounterImpl());
        try {
            assertThatThrownBy(restarted::start)
                    .hasStackTraceContaining("CRC mismatch");
        } finally {
            restarted.stop();
        }
    }

    private void writeAndSnapshot(int writes) throws Exception {
        JGroupsRaft<Counter> raft = buildRaft(new CounterImpl());
        try {
            raft.start();
            assertThat(eventually(() -> raft.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();

            for (int i = 0; i < writes; i++) {
                raft.write(c -> c.add(1));
            }

            raft.administration().snapshot().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } finally {
            raft.stop();
        }
    }

    /**
     * Flips the last byte of the snapshot data, keeping the header and the CRC trailer intact.
     */
    private void corruptSnapshotData() throws IOException {
        Path snapshot = findSnapshotFile();
        try (RandomAccessFile file = new RandomAccessFile(snapshot.toFile(), "rw")) {
            long position = file.length() - SnapshotStorage.CRC_SIZE - 1;
            assertThat(position).isGreaterThanOrEqualTo(SnapshotStorage.SNAPSHOT_HEADER_SIZE);

            file.seek(position);
            int original = file.read();
            file.seek(position);
            file.write(original ^ 0xFF);
        }
    }

    private Path findSnapshotFile() throws IOException {
        try (Stream<Path> walk = Files.walk(logDir)) {
            List<Path> found = walk
                    .filter(p -> p.getFileName().toString().equals(SnapshotStorage.SNAPSHOT_FILE_NAME))
                    .toList();
            assertThat(found).hasSize(1);
            return found.get(0);
        }
    }

    private JGroupsRaft<Counter> buildRaft(CounterImpl sm) {
        return JGroupsRaft.builder(sm, Counter.class)
                .withJGroupsConfig("test-raft.xml")
                .withClusterName("corrupted-snapshot-" + System.nanoTime())
                .configureRaft()
                    .withRaftId("node")
                    .withMembers(Collections.singletonList("node"))
                    .withLogClass(FileBasedLog.class)
                    .withLogDirectory(logDir.toString())
                    .withUseFsync(false)
                    .and()
                .build();
    }

    @JGroupsRaftStateMachine
    interface Counter {

        @StateMachineWrite(id = 1)
        Void add(int value);

        @StateMachineRead(id = 2)
        int get();
    }

    static final class CounterImpl implements Counter {

        @StateMachineField(order = 0)
        private int value;

        @Override
        public Void add(int value) {
            this.value += value;
            return null;
        }

        @Override
        public int get() {
            return value;
        }
    }
}
