package org.jgroups.tests.harness;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

public final class RaftAssertion {

    private RaftAssertion() { }

    public static void assertLeaderlessOperationThrows(ThrowingRunnable operation) {
        assertLeaderlessOperationThrows(operation, "Running operation without a leader.");
    }

    public static void assertLeaderlessOperationThrows(ThrowingRunnable operation, String message) {
        assertLeaderlessOperationThrows(operation, () -> message);
    }

    public static void assertLeaderlessOperationThrows(ThrowingRunnable operation, Supplier<String> message) {
        Assertions.assertThatThrownBy(operation::run)
                .as(message)
                .satisfiesAnyOf(
                        // In case the leader already received the view update and stepped down.
                        tc -> assertThat(tc)
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("I'm not the leader "),

                        // In case the request is sent before the leader step down.
                        // We could update this so when the leader step down it cancel requests.
                        tc -> assertThat(tc).isInstanceOf(TimeoutException.class),

                        // The request was sent but failed.
                        tc -> assertThat(tc).isInstanceOf(ExecutionException.class)
                                .cause()
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("I'm not the leader ")
                );
    }

    public static void assertCommitIndex(long timeout, long expected_commit, long expected_applied, Function<JChannel, RAFT> converter, Collection<JChannel> channels) {
        BooleanSupplier bs = () -> {
            boolean all_ok = true;
            for (JChannel ch : channels) {
                RAFT raft = converter.apply(ch);
                if (expected_commit != raft.commitIndex() || expected_applied != raft.lastAppended())
                    all_ok = false;
            }
            return all_ok;
        };
        assertThat(eventually(bs, timeout, TimeUnit.MILLISECONDS))
                .as("Commit indexes never matched")
                .isTrue();

        for (JChannel ch : channels) {
            RAFT raft = converter.apply(ch);
            String check = String.format("%s: last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.lastAppended(), raft.commitIndex());
            System.out.printf(check);
            assertThat(raft)
                    .as(check)
                    .returns(expected_commit, RAFT::commitIndex)
                    .returns(expected_applied, RAFT::lastAppended);
        }
    }

    public static void assertCommitIndex(long timeout, long expected_commit, long expected_applied, Function<JChannel, RAFT> converter, JChannel... channels) {
        assertCommitIndex(timeout, expected_commit, expected_applied, converter, List.of(channels));
    }

    public static void waitUntilAllRaftsHaveLeader(JChannel[] channels, Function<JChannel, RAFT> converter) {
        RAFT[] rafts = Arrays.stream(channels)
                .filter(Objects::nonNull)
                .map(converter)
                .toArray(RAFT[]::new);
        BaseRaftElectionTest.waitUntilLeaderElected(rafts, 10_000);
    }

    @FunctionalInterface
    public interface ThrowingRunnable {

        void run() throws Throwable;
    }
}
