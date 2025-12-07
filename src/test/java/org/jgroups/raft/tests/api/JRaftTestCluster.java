package org.jgroups.raft.tests.api;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftRole;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.tests.harness.BaseRaftChannelTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

public final class JRaftTestCluster<T extends StateMachine> {

    private static final AtomicInteger CLUSTER_ID = new AtomicInteger(0);

    private final JRaftTest<T> delegate;

    private JRaftTestCluster(Supplier<T> stateMachineFactory, Class<T> clazz, int size) {
        try {
            this.delegate = new JRaftTest<>(stateMachineFactory, clazz, size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends StateMachine> JRaftTestCluster<T> create(Supplier<T> stateMachineFactory, Class<T> clazz, int size) {
        return new JRaftTestCluster<>(stateMachineFactory, clazz, size);
    }

    public JGroupsRaft<T> raft(int index) {
        assert index < delegate.rafts.length : "Index out of bounds, maximum is " + (delegate.rafts.length - 1);
        return delegate.rafts[index];
    }

    public T stateMachine(int index) {
        assert index < delegate.stateMachines.length : "Index out of bounds, maximum is " + (delegate.stateMachines.length - 1);
        return delegate.stateMachines[index];
    }

    public void waitUntilLeaderElected() {
        assertThat(eventually(() -> Stream.of(delegate.rafts)
                .anyMatch(r -> r.role() == JGroupsRaftRole.LEADER), 10, TimeUnit.SECONDS))
                .isTrue();
        assertThat(eventually(() -> Stream.of(delegate.rafts)
                .allMatch(r -> r.state().leader() != null), 10, TimeUnit.SECONDS))
                .isTrue();
        assertThat(Stream.of(delegate.rafts).filter(r -> r.role() == JGroupsRaftRole.LEADER).count()).isOne();
    }

    public JGroupsRaft<T> leader() {
        return Stream.of(delegate.rafts)
                .filter(r -> r.role() == JGroupsRaftRole.LEADER)
                .findFirst().orElse(null);
    }

    public JGroupsRaft<T> follower() {
        return Stream.of(delegate.rafts)
                .filter(r -> r.role() == JGroupsRaftRole.FOLLOWER)
                .findAny().orElse(null);
    }

    public int leaderIndex() {
        for (int i = 0; i < delegate.rafts.length; i++) {
            if (delegate.rafts[i].role() == JGroupsRaftRole.LEADER) {
                return i;
            }
        }
        throw new IllegalStateException("No leader found in the cluster");
    }

    public JChannel channel(int index) {
        assert index < delegate.channels.length : "Index out of bounds, maximum is " + (delegate.channels.length - 1);
        return delegate.channels[index];
    }

    public RAFT raftProtocol(int index) {
        JChannel ch = channel(index);
        return RAFT.findProtocol(RAFT.class, ch.getProtocolStack().getTopProtocol(), true);
    }

    public void close() throws Exception {
        for (JGroupsRaft<T> raft : delegate.rafts) {
            raft.stop();
        }
        delegate.destroy();
    }

    private static final class JRaftTest<T extends StateMachine> extends BaseRaftChannelTest {

        private final T[] stateMachines;
        private final JChannel[] channels;
        private final Class<T> clazz;
        private final JGroupsRaft<T>[] rafts;
        private final String clusterName;

        @SuppressWarnings("unchecked")
        JRaftTest(Supplier<T> stateMachineFactory, Class<T> clazz, int size) throws Exception {
            this.clusterSize = size;
            this.clazz = clazz;
            this.clusterName = "cluster-test-" + CLUSTER_ID.getAndIncrement();
            createCluster(size);

            stateMachines = (T[]) new StateMachine[size];
            rafts = new JGroupsRaft[size];
            channels = new JChannel[size];

            List<String> participants = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                participants.add(Character.toString('A' + i));
            }

            for (int i = 0; i < size; i++) {
                T sm = stateMachineFactory.get();
                channels[i] = channel(i);
                JGroupsRaft<T> raft = JGroupsRaft.builder(sm, clazz)
                        .withJChannel(channels[i])
                        .registerSerializationContextInitializer(new TestSerializationInitializerImpl())
                        .configureRaft()
                            .withRaftId(Character.toString('A' + i))
                            .withMembers(participants)
                            .withLogClass(InMemoryLog.class)
                            .and()
                        .build();

                raft.start();
                this.rafts[i] = raft;
                this.stateMachines[i] = sm;
            }
        }

        @Override
        protected String clusterName() {
            return clusterName;
        }

        public void destroy() throws Exception {
            destroyCluster();
        }
    }
}
