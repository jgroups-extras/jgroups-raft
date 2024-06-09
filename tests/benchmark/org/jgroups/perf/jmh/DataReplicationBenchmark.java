package org.jgroups.perf.jmh;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark the replication throughput.
 *
 * @author José Bolina
 * @since 1.0.13
 */
@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 10, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 3, jvmArgsPrepend = "-Djava.net.preferIPv4Stack=true -Djgroups.udp.ip_ttl=0 -Djmh.executor=PLATFORM")
public class DataReplicationBenchmark {

    @Benchmark
    public byte[] testReplication(ClusterState state) throws Exception {
        return state.leader.set(state.data, 0, state.dataSize);
    }

    @State(Scope.Benchmark)
    public static class ClusterState {

        // Change the majority between values.
        @Param({"3", "5"})
        public int clusterSize;

        // Keep a factor of 4 between values.
        @Param({"256", "1024", "4096"})
        public int dataSize;

        // Storage implementations that write to disk can enable/disable fsync.
        @Param({"false", "true"})
        public boolean useFsync;

        public byte[] data;

        public RaftHandle[] members;

        public RaftHandle leader;


        @Setup
        public void initialize() throws Exception {
            List<String> memberList = IntStream.range(0, clusterSize)
                    .mapToObj(i -> Character.toString('A' + i))
                    .collect(Collectors.toList());

            members = new RaftHandle[clusterSize];
            for (int i = 0; i < clusterSize; i++) {
                String name = Character.toString('A' + i);

                // Utilize the default configuration shipped with jgroups-raft.
                JChannel ch = new JChannel("raft-benchmark.xml");
                ch.name(name);

                // A no-op state machine.
                RaftHandle handler = new RaftHandle(ch, new EmptyStateMachine(dataSize));
                RAFT raft = handler.raft();

                // Default configuration.
                raft.raftId(name);
                raft.members(memberList);

                // Fine-tune the RAFT protocol below.
                raft.logClass(FileBasedLog.class.getCanonicalName())
                        .logDir(String.format("%s/target/benchmark", System.getProperty("user.dir")))
                        .logUseFsync(useFsync);

                members[i] = handler;
                ch.connect("jmh-replication");
            }

            data = new byte[dataSize];
            ThreadLocalRandom.current().nextBytes(data);

            // Block until ALL members have a leader installed.
            BooleanSupplier bs = () -> Arrays.stream(members)
                    .map(RaftHandle::raft)
                    .allMatch(r -> r.leader() != null);
            Supplier<String> message = () -> Arrays.stream(members)
                    .map(RaftHandle::raft)
                    .map(r -> String.format("%s: %s", r.raftId(), r.leader()))
                    .collect(Collectors.joining(System.lineSeparator()));
            assert RaftTestUtils.eventually(bs, 1, TimeUnit.MINUTES) : message.get();

            for (RaftHandle member : members) {
                if (member.isLeader()) {
                    leader = member;
                    break;
                }
            }

            assert leader != null : "Leader not found";
        }

        @TearDown
        public void tearDown() throws Exception {
            for (int i = clusterSize - 1; i >= 0; i--) {
                RaftHandle rh = members[i];
                Util.close(rh.channel());
            }

            for (RaftHandle member : members) {
                RaftTestUtils.deleteRaftLog(member.raft());
            }
        }
    }

    private static final class EmptyStateMachine implements StateMachine {
        private static final byte[] RESPONSE = new byte[0];

        private final int dataSize;

        public EmptyStateMachine(int dataSize) {
            this.dataSize = dataSize;
        }

        @Override
        public byte[] apply(byte[] data, int offset, int length, boolean serialize_response) throws Exception {
            if (data.length != dataSize)
                throw new IllegalArgumentException(String.format("Data size does not match: %d != %d", data.length, dataSize));

            return RESPONSE;
        }

        @Override
        public void readContentFrom(DataInput in) throws Exception { }

        @Override
        public void writeContentTo(DataOutput out) throws Exception { }
    }
}
