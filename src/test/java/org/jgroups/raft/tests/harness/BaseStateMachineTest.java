package org.jgroups.raft.tests.harness;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

/**
 * Base class for tests that use a {@link StateMachine}.
 *
 * <p>
 * This class provides base functionalities to assert on the {@link StateMachine}, and easy methods to retrieve the
 * instances.
 * </p>
 *
 * <h2>Contract</h2>
 *
 * <p>
 * All the subclasses <b>must</b> override the {@link #createStateMachine(JChannel)} method. This is invoked everytime
 * a new node is added to the cluster. A method to the super is necessary when overriding
 * {@link #beforeClusterCreation()} or {@link #beforeChannelConnection(JChannel)}. These methods are responsible for
 * populating the {@link RaftHandle}s and {@link StateMachine}s.
 * </p>
 *
 * @param <T>: The type of the {@link StateMachine} the test uses.
 * @since 1.0.13
 * @see BaseRaftChannelTest
 */
public class BaseStateMachineTest<T extends StateMachine> extends BaseRaftChannelTest {

    // Keep track of all handles in the cluster
    private RaftHandle[] handles;

    @Override
    protected void beforeClusterCreation() throws Exception {
        this.handles = new RaftHandle[clusterSize];
    }

    /**
     * Timeout when verifying if the state machines match.
     */
    protected long matchTimeout = 10_000;

    @Override
    protected void beforeChannelConnection(JChannel ch) {
        int i = 0;
        for (JChannel channel : actualChannels()) {
            if (channel == ch) break;
            i += 1;
        }
        handles[i] = new RaftHandle(ch, createStateMachine(ch));
    }

    /**
     * Creates the {@link StateMachine} instance to use during the tests.
     * <p>
     * <b>Warning:</b> All subclasses <b>must</b> override this method.
     * </p>
     *
     * @param ch: The channel to retrieve any information to create the state machine instance.
     * @return A {@link StateMachine} instance to use in the tests.
     */
    protected T createStateMachine(JChannel ch) {
        throw new IllegalStateException("Unknown state machine");
    }

    /**
     * Retrieve the {@link StateMachine} in the given index.
     *
     * @param index: Index to retrieve the {@link RAFT} to retrieve the {@link StateMachine}.
     * @return The {@link StateMachine} cast to type T.
     * @throws AssertionError in case the {@link RAFT} instance for the index does not exist.
     */
    protected final T stateMachine(int index) {
        RAFT r = raft(index);

        assert r != null : "RAFT should not be null!";
        return stateMachine(r);
    }

    /**
     * Retrieves the {@link StateMachine} in use in the {@link RAFT} {@param r} instance.
     *
     * @param r: {@link RAFT} instance to retrieve the {@link StateMachine}.
     * @return The {@link StateMachine} cast to type T.
     */
    @SuppressWarnings("unchecked")
    protected final T stateMachine(RAFT r) {
        return (T) r.stateMachine();
    }

    protected final RaftHandle handle(int index) {
        return handles[index];
    }

    /**
     * Assert that all the {@link StateMachine} in the given {@param indexes} are matching.
     * First, verify that all members have a matching commit index, meaning everything was applied. Then, verify
     * the actual state machines.
     *
     * @param indexes: Indexes to verify. The {@link JChannel} for each index must exist.
     */
    protected final void assertStateMachineEventuallyMatch(int ... indexes) {
        // Verifies the commit indexes of all nodes.
        BooleanSupplier commitIndexVerify = () -> Arrays.stream(indexes)
                .mapToObj(this::raft)
                .map(Objects::requireNonNull)
                .map(RAFT::commitIndex)
                .distinct()
                .count() == 1;

        assertThat(eventually(commitIndexVerify, matchTimeout, TimeUnit.MILLISECONDS))
                .as(generateErrorMessage())
                .isTrue();
        LOGGER.info(dumpStateMachines(indexes));

        // Verify the state machines match.
        BooleanSupplier matchStateMachineVerify = () -> Arrays.stream(indexes)
                .mapToObj(this::stateMachine)
                .distinct()
                .count() == 1;
        assertThat(eventually(matchStateMachineVerify, matchTimeout, TimeUnit.MILLISECONDS))
                .as(generateErrorMessage() + " where " + dumpStateMachines(indexes))
                .isTrue();
    }

    private String generateErrorMessage() {
        StringBuilder sb = new StringBuilder();
        RAFT leader = leader();

        assertThat(leader)
                .as("Expecting an elected leader")
                .isNotNull();

        sb.append(leader.raftId())
                .append(": commit-index=").append(leader.commitIndex())
                .append(leader.dumpCommitTable());
        return sb.toString();
    }

    private String dumpStateMachines(int ... indexes) {
        StringBuilder sb = new StringBuilder();
        for (int i : indexes) {
            sb.append(raft(i).raftId())
                    .append(" -> ")
                    .append(stateMachine(i))
                    .append("\n");
        }
        return sb.toString();
    }
}
