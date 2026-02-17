package org.jgroups.perf.replication;

import org.jgroups.annotations.Property;
import org.jgroups.perf.CommandLineOptions;
import org.jgroups.perf.harness.AbstractRaftBenchmark;
import org.jgroups.perf.harness.RaftBenchmark;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Field;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test performance of replicating data.
 * <p>
 * This benchmark utilizes the base {@link RaftHandle} to verify the replication performance of a configurable-sized
 * byte array. The test verifies only the replication part, where the state machine does not interpret the bytes.
 * Since the {@link StateMachine} implementation is application-specific, we don't measure it in our tests.
 * </p>
 * <p>
 * The benchmark accepts configuration for the payload size, whether fsync the log.
 * </p>
 *
 * @author José Bolina
 */
public class ReplicationPerf extends AbstractRaftBenchmark {

    private static final Field DATA_SIZE, USE_FSYNC;

    static {
        try {
            DATA_SIZE = Util.getField(ReplicationPerf.class, "data_size", true);
            USE_FSYNC = Util.getField(ReplicationPerf.class, "use_fsync", true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final RaftHandle raft;
    private final CounterStateMachine csm;

    @Property
    protected int data_size = 526;

    @Property
    protected boolean use_fsync;

    public ReplicationPerf(CommandLineOptions cmd) throws Throwable {
        super(cmd);
        this.csm = new CounterStateMachine();
        this.raft = new RaftHandle(channel, csm);
    }

    @Override
    public RaftBenchmark syncBenchmark(ThreadFactory tf) {
        visitRaftBeforeBenchmark();
        return new SyncReplicationBenchmark(num_threads, raft, createTestPayload(), tf);
    }

    @Override
    public RaftBenchmark asyncBenchmark(ThreadFactory tf) {
        visitRaftBeforeBenchmark();
        return new AsyncReplicationBenchmark(num_threads, raft, createTestPayload(), tf);
    }

    @Override
    public String extendedEventLoopHeader() {
        return String.format("[s] Data size (%d bytes) [f] Use fsync (%b)", data_size, use_fsync);
    }

    @Override
    public void extendedEventLoop(int c) throws Throwable {
        switch (c) {
            case 's':
                changeFieldAcrossCluster(DATA_SIZE, Util.readIntFromStdin("new data size: "));
                break;

            case 'f':
                changeFieldAcrossCluster(USE_FSYNC, !use_fsync);
                break;

            default:
                System.out.printf("Unknown option: %c%n", c);
                break;
        }
    }

    @Override
    public void clear() {
        try {
            RaftTestUtils.deleteRaftLog(channel.getProtocolStack().findProtocol(RAFT.class));
        } catch (Exception e) {
            System.err.printf("Failed deleting log file: %s", e);
        }
    }

    private void visitRaftBeforeBenchmark() {
        raft.raft().logUseFsync(use_fsync);
    }

    private byte[] createTestPayload() {
        byte[] payload = new byte[data_size];
        ThreadLocalRandom.current().nextBytes(payload);
        return payload;
    }

    private static class CounterStateMachine implements StateMachine {
        private long updates;


        @Override
        public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) {
            updates++;
            return null;
        }

        @Override
        public void readContentFrom(DataInput in) { }

        @Override
        public void writeContentTo(DataOutput out) throws Exception { }
    }
}
