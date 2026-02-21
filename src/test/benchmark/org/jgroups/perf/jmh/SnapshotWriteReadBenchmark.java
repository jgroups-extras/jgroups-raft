package org.jgroups.perf.jmh;

import org.jgroups.JChannel;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineField;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.internal.serialization.binary.SerializationRegistry;
import org.jgroups.raft.internal.statemachine.StateMachineWrapper;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 2, jvmArgsPrepend = "-Djmh.executor=PLATFORM")
public class SnapshotWriteReadBenchmark {

    @Benchmark
    public DataOutput createSnapshot(BenchmarkState state) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(128, true);
        state.getStateMachine().writeContentTo(out);
        return out;
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private static final String MODE_AUTO = "auto";
        private static final String MODE_MANUAL = "manual";

        @Param({"50000"})
        public int numberEntries;

        @Param({MODE_AUTO, MODE_MANUAL})
        public String mode;

        private StateMachine sm;

        @Setup
        public void initialize() throws Exception {
            if (Objects.equals(mode, MODE_MANUAL)) {
                setupOld();
                return;
            }

            if (Objects.equals(mode, MODE_AUTO)) {
                setupNew();
                return;
            }

            throw new IllegalStateException("unknown mode: " + mode);
        }

        public StateMachine getStateMachine() {
            return sm;
        }

        private void setupOld() throws Exception {
            Protocol[] stack = new Protocol[] { new SHARED_LOOPBACK(), new RAFT() };
            ReplicatedStateMachine<String, String> rsm = new ReplicatedStateMachine<>(new JChannel(stack));
            for (int i = 0; i < numberEntries; i++) {
                byte[] buff = createApply();
                rsm.apply(buff, 0, buff.length, false);
            }
            sm = rsm;
        }

        private void setupNew() {
            SampleNew sn = new SampleNew();

            for (int i = 0; i < numberEntries; i++) {
                sn.state.put(string(), string());
            }

            SerializationRegistry registry = SerializationRegistry.create();
            sm = new StateMachineWrapper<>(sn, SampleNew.class, null, Serializer.create(registry));
        }

        private byte[] createApply() throws Exception {
            ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(256);
            out.writeByte(1);
            Util.objectToStream(string(), out);
            Util.objectToStream(string(), out);

            return out.buffer();
        }

        private static String string() {
            return UUID.randomUUID().toString();
        }
    }

    @JGroupsRaftStateMachine
    private static final class SampleNew {
        @StateMachineField(order = 0)
        Map<String, String> state = new HashMap<>();
    }


}
