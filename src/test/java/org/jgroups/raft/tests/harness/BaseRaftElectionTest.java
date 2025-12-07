package org.jgroups.raft.tests.harness;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.testfwk.MockRaftCluster;
import org.jgroups.raft.testfwk.RaftNode;
import org.jgroups.raft.testfwk.RaftTestUtils;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.testng.annotations.DataProvider;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for election tests.
 * <p>
 * This class itself is final, but it has two inner classes for tests based either on channels or the mock cluster.
 * The {@link ChannelBased} or {@link ClusterBased}, respectively. Subclasses must choose which approach to use and
 * extend one of the inner classes.
 * </p>
 *
 * <p>
 * This class contains some utilities for election, like retrieving the protocol or waiting for something related to
 * the election mechanism. There is no methods required for implementation.
 * </p>
 *
 * @since 1.0.13
 * @see AbstractRaftTest
 */
public final class BaseRaftElectionTest {

    /**
     * A data provider to utilize with the {@link org.testng.annotations.Test} annotation.
     * <p>
     * This data provider pass as argument the classes which implement an election algorithm.
     * </p>
     */
    public static final String ALL_ELECTION_CLASSES_PROVIDER = "all-election-classes";

    private static Object[][] electionClasses() {
        return new Object[][] {
                {ELECTION.class},
                {ELECTION2.class},
        };
    }

    /**
     * Asserts that something eventually evaluates to <code>true</code>.
     *
     * @param timeout: The timeout in milliseconds to wait.
     * @param bs: The expression to evaluate.
     * @param message: The error message in case of failure.
     */
    private static void assertWaitUntil(long timeout, BooleanSupplier bs, Supplier<String> message) {
        assertThat(RaftTestUtils.eventually(bs, timeout, TimeUnit.MILLISECONDS))
                .as(message)
                .isTrue();
    }

    /**
     * Wait until the voting thread stops running on all election instances.
     *
     * @param elections: The elections classes to verify.
     * @param timeout: Time in milliseconds to wait.
     */
    private static void waitUntilVotingThreadStop(BaseElection[] elections, long timeout) {
        // Wait until the voting thread stops.
        // In theory, the voting thread runs only on the view coordinator, nevertheless we iterate over all member.
        BooleanSupplier bs = () -> Arrays.stream(elections)
                .filter(Objects::nonNull)
                .noneMatch(BaseElection::isVotingThreadRunning);
        Supplier<String> message = () -> Arrays.stream(elections)
                .filter(Objects::nonNull)
                .map(e -> e.raft().raftId() + " voting running? " + e.isVotingThreadRunning())
                .collect(Collectors.joining(System.lineSeparator()));

        assertWaitUntil(timeout, bs, message);
    }

    /**
     * Wait and assert a leader is elected in the given {@link RAFT}s.
     * <p>
     * This method waits until a leader is elected between the given {@link RAFT}s instances. A leader is elected once:
     * <ul>
     *     <li>A majority of members see the same leader;</li>
     *     <li>A majority of members have the same term;</li>
     *     <li>The leader seen by everyone is in the {@link RAFT} instances.</li>
     * </ul>
     * </p>
     *
     * @param rafts: The instances to check for an elected leader.
     * @param timeout: The timeout in milliseconds to wait for the election.
     */
    public static void waitUntilLeaderElected(RAFT[] rafts, long timeout) {
        Supplier<String> message = () -> Arrays.stream(rafts)
                .map(r -> String.format("%s: %d -> %s", r.raftId(), r.currentTerm(), r.leader()))
                .collect(Collectors.joining(System.lineSeparator()))
                + "\n with a majority of " + rafts[0].majority();
        assertWaitUntil(timeout, checkRaftLeader(rafts), message);
    }

    /**
     * Blocks until a new leader is elected between the provided instances and is received by all participants.
     *
     * @param rafts: Instances to check.
     * @param timeout: The timeout in milliseconds to wait.
     * @see #waitUntilLeaderElected(RAFT[], long)
     */
    public static void waitUntilAllHaveLeaderElected(RAFT[] rafts, long timeout) {
        waitUntilLeaderElected(rafts, timeout);
        BooleanSupplier bs = () -> Arrays.stream(rafts)
                .filter(Objects::nonNull)
                .allMatch(r -> r.leader() != null);
        Supplier<String> message = () -> Arrays.stream(rafts)
                .map(r -> String.format("%s: %d -> %s", r.raftId(), r.currentTerm(), r.leader()))
                .collect(Collectors.joining(System.lineSeparator()));
        assertWaitUntil(timeout, bs, message);
    }

    private static BooleanSupplier checkRaftLeader(RAFT[] rafts) {
        int majority = rafts[0].majority();

        assertThat(rafts).hasSizeGreaterThanOrEqualTo(majority);

        // Check that a majority of nodes have the same leader.
        BooleanSupplier sameLeader = () -> Arrays.stream(rafts)
                .map(RAFT::leader)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .values().stream()
                .max(Long::compareTo)
                .orElse(Long.MIN_VALUE) >= majority;

        // Check that a majority of members have the same term.
        BooleanSupplier sameTerm = () -> {
            long currentTerm = Arrays.stream(rafts)
                    .map(RAFT::currentTerm)
                    .max(Long::compareTo)
                    .orElseThrow();
            long counter = 0;
            for (RAFT raft : rafts) {
                if (raft.currentTerm() == currentTerm) counter++;
            }
            return counter >= majority;
        };

        // Check that the node seen as leader is present in the cluster and sees itself as leader.
        // This is necessary when checking for a new leader between elections. Say we had a leader, stop some nodes,
        // and check again. The nodes could see the past leader still and evaluate to true.
        BooleanSupplier electedIsLeader = () -> Arrays.stream(rafts)
                .map(RAFT::leader)
                .filter(Objects::nonNull)
                .distinct()
                .map(l -> Arrays.stream(rafts)
                        .filter(r -> r.getAddress().equals(l))
                        .findFirst())
                .allMatch(o -> {
                    if (o.isEmpty()) return false;
                    RAFT r = o.get();
                    return r.isLeader();
                });

        return () -> sameLeader.getAsBoolean()
                && sameTerm.getAsBoolean()
                && electedIsLeader.getAsBoolean();
    }

    /**
     * Internal class utilized to instantiate the concrete {@link BaseElection} instance.
     * <p>
     * This internal class receives the data provider argument from {@link #ALL_ELECTION_CLASSES_PROVIDER} and
     * creates a new instance.
     * </p>
     */
    private static class ClassHandler {
        protected Class<? extends BaseElection> electionClass;

        @SuppressWarnings("unchecked")
        protected void setElectionClass(Object[] args) {
            electionClass = (Class<? extends BaseElection>) args[0];
        }

        public BaseElection instantiate() throws Exception {
            assert electionClass != null : "Election class not set";
            // The default constructor is always available.
            return electionClass.getDeclaredConstructor().newInstance();
        }
    }

    /**
     * Base class for election tests based on {@link JChannel}.
     * <p>
     * This class builds upon {@link BaseRaftChannelTest} and offer utilities specific to the election process.
     * </p>
     */
    public static class ChannelBased extends BaseRaftChannelTest {

        private final ClassHandler handler = new ClassHandler();

        @Override
        protected void passDataProviderParameters(Object[] args) {
            handler.setElectionClass(args);
        }

        @Override
        protected BaseElection createNewElection() throws Exception {
            return handler.instantiate();
        }

        /**
         * Stop the voting thread manually.
         */
        protected final void stopVotingThread() {
            for (JChannel channel : channels()) {
                if (channel == null) continue;

                BaseElection election = election(channel);
                election.stopVotingThread();
            }
        }

        /**
         * Retrieves the concrete {@link BaseElection} implementation in the protocol stack.
         *
         * @param ch: Channel to retrieve the election protocol.
         * @return The {@link BaseElection} concrete implementation or <code>null</code>, if not found.
         */
        protected final BaseElection election(JChannel ch) {
            return ch.getProtocolStack().findProtocol(handler.electionClass);
        }

        /**
         * Wait until a leader is elected in the cluster.
         * <p>
         * This methods blocks and waits until all the requested members in the cluster have an elected leader.
         * </p>
         *
         * @param timeout: Time in milliseconds to wait for an election.
         * @param indexes: The index of the cluster members.
         * @see #waitUntilLeaderElected(RAFT[], long)
         */
        protected final void waitUntilLeaderElected(long timeout, int ... indexes) {
            RAFT[] rafts = Arrays.stream(indexes)
                    .mapToObj(this::raft)
                    .filter(Objects::nonNull)
                    .toArray(RAFT[]::new);
            BaseRaftElectionTest.waitUntilLeaderElected(rafts, timeout);
        }

        /**
         * Wait until the voting thread stops running on all election instances.
         *
         * @param timeout: Time in milliseconds to wait.
         * @param indexes: The index of the cluster members to check.
         */
        protected final void waitUntilVotingThreadStops(long timeout, int ... indexes) {
            BaseElection[] elections = Arrays.stream(indexes)
                    .mapToObj(this::channel)
                    .filter(Objects::nonNull)
                    .map(this::election)
                    .toArray(BaseElection[]::new);
            waitUntilVotingThreadStop(elections, timeout);
        }

        @DataProvider(name = ALL_ELECTION_CLASSES_PROVIDER)
        protected static Object[][] electionClasses() {
            return BaseRaftElectionTest.electionClasses();
        }
    }

    /**
     * Base class for election tests based on a {@link MockRaftCluster}.
     * <p>
     * This class builds on the base {@link BaseRaftClusterTest} and offer utilities specific to the election process.
     * </p>
     *
     * @param <T>: The type of {@link MockRaftCluster}.
     */
    public abstract static class ClusterBased<T extends MockRaftCluster> extends BaseRaftClusterTest<T> {

        private final ClassHandler handler = new ClassHandler();

        @Override
        protected void passDataProviderParameters(Object[] args) {
            handler.setElectionClass(args);
        }

        @Override
        protected BaseElection createNewElection() throws Exception {
            return handler.instantiate();
        }

        /**
         * Retrieves the concrete {@link BaseElection} algorithm from the cluster member.
         *
         * @param index: The member index in the cluster.
         * @return The concrete election algorithm.
         */
        protected final BaseElection election(int index) {
            RaftNode node = node(index);
            return node.election();
        }

        /**
         * Retrieves the election protocol of all cluster members.
         *
         * @return The election protocol of all members in the cluster. There is no <code>null</code> entries.
         */
        protected BaseElection[] elections() {
            return Arrays.stream(nodes())
                    .filter(Objects::nonNull)
                    .map(RaftNode::election)
                    .toArray(BaseElection[]::new);

        }

        /**
         * Wait until a leader is elected in the cluster.
         * <p>
         * This methods blocks and waits until all the requested members in the cluster have an elected leader.
         * </p>
         *
         * @param timeout: Time in milliseconds to wait for an election.
         * @param indexes: The index of the cluster members.
         * @see #waitUntilLeaderElected(RAFT[], long)
         */
        protected void waitUntilLeaderElected(long timeout, int ... indexes) {
            RAFT[] rafts = Arrays.stream(indexes)
                    .mapToObj(this::node)
                    .filter(Objects::nonNull)
                    .map(RaftNode::raft)
                    .toArray(RAFT[]::new);
            BaseRaftElectionTest.waitUntilLeaderElected(rafts, timeout);
        }

        /**
         * Wait until the voting thread stops running on all election instances.
         *
         * @param timeout: Time in milliseconds to wait.
         * @param indexes: The index of the cluster members to check.
         */
        protected void waitUntilVotingThreadStops(long timeout, int ... indexes) {
            BaseElection[] elections = Arrays.stream(indexes)
                    .mapToObj(this::node)
                    .filter(Objects::nonNull)
                    .map(RaftNode::election)
                    .toArray(BaseElection[]::new);
            waitUntilVotingThreadStop(elections, timeout);
        }

        @DataProvider(name = ALL_ELECTION_CLASSES_PROVIDER)
        protected static Object[][] electionClasses() {
            return BaseRaftElectionTest.electionClasses();
        }
    }
}
