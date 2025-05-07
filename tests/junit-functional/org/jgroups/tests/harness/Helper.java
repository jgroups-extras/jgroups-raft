package org.jgroups.tests.harness;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

public final class Helper {

    private Helper() { }

    public static void assertCommitIndex(long timeout, long expected_commit, long expected_applied, Function<JChannel, RAFT> converter, JChannel... channels) {
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

    public static void waitUntilAllRaftsHaveLeader(JChannel[] channels, Function<JChannel, RAFT> converter) {
        RAFT[] rafts = Arrays.stream(channels)
                .filter(Objects::nonNull)
                .map(converter)
                .toArray(RAFT[]::new);
        BaseRaftElectionTest.waitUntilLeaderElected(rafts, 10_000);
    }
}
