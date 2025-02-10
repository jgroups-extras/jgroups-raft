package org.jgroups.perf.jmh;

import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftRole;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineWrite;
import org.jgroups.raft.testfwk.RaftTestUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
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

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 10, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 3, jvmArgsPrepend = "-Djava.net.preferIPv4Stack=true -Djgroups.udp.ip_ttl=0 -Djmh.executor=PLATFORM")
public class NewApiReplicationBenchmark {

    @Benchmark
    public byte[] testReplication(ClusterState state) {
        return state.leader.write((Function<BenchmarkStateMachine, byte[]>) bsm -> bsm.replicate(state.data));
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

        public JGroupsRaft<BenchmarkStateMachine>[] members;

        public JGroupsRaft<BenchmarkStateMachine> leader;


        @Setup
        public void initialize() {
            List<String> memberList = IntStream.range(0, clusterSize)
                    .mapToObj(i -> Character.toString('A' + i))
                    .collect(Collectors.toList());

            members = new JGroupsRaft[clusterSize];
            for (int i = 0; i < clusterSize; i++) {
                String name = Character.toString('A' + i);

                BenchmarkStateMachine sm = new EmptyStateMachine(dataSize);

                JGroupsRaft<BenchmarkStateMachine> node = JGroupsRaft.builder(sm, BenchmarkStateMachine.class)
                        .withJGroupsConfig("raft-benchmark.xml")
                        .withClusterName("jmh-replication")
                        .configureRaft()
                            .withRaftId(name)
                            .withMembers(memberList)
                            .withLogClass(FileBasedLog.class)
                            .withLogDirectory(String.format("%s/target/benchmark", System.getProperty("user.dir")))
                            .withUseFsync(useFsync)
                            .and()
                        .build();

                members[i] = node;
                node.start();
            }

            data = new byte[dataSize];
            ThreadLocalRandom.current().nextBytes(data);

            // Block until ALL members have a leader installed.
            BooleanSupplier bs = () -> Arrays.stream(members)
                    .allMatch(r -> r.state().leader() != null);
            Supplier<String> message = () -> Arrays.stream(members)
                    .map(r -> String.format("%s: %s", r.state().id(), r.state().leader()))
                    .collect(Collectors.joining(System.lineSeparator()));
            assert RaftTestUtils.eventually(bs, 1, TimeUnit.MINUTES) : message.get();

            for (JGroupsRaft<BenchmarkStateMachine> member : members) {
                if (member.role() == JGroupsRaftRole.LEADER) {
                    leader = member;
                    break;
                }
            }

            assert leader != null : "Leader not found";
        }

        @TearDown
        public void tearDown() throws Exception {
            for (int i = clusterSize - 1; i >= 0; i--) {
                JGroupsRaft<BenchmarkStateMachine> rh = members[i];
                rh.stop();
            }

            deleteDir(Path.of(String.format("%s/target/benchmark", System.getProperty("user.dir"))).toFile());
        }
    }

    private static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }

    @JGroupsRaftStateMachine
    public interface BenchmarkStateMachine extends StateMachine {

        @StateMachineWrite(id = 1)
        byte[] replicate(byte[] data);
    }

    private record EmptyStateMachine(int dataSize) implements BenchmarkStateMachine {
        private static final byte[] RESPONSE = new byte[0];

        @Override
        public void readContentFrom(DataInput in) { }

        @Override
        public void writeContentTo(DataOutput out) { }

        @Override
        public byte[] replicate(byte[] data) {
            if (data.length != dataSize)
                throw new IllegalArgumentException(String.format("Data size does not match: %d != %d", data.length, dataSize));

            return RESPONSE;
        }
    }
}
