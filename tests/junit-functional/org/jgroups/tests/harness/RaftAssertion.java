package org.jgroups.tests.harness;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

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

    @FunctionalInterface
    public interface ThrowingRunnable {

        void run() throws Throwable;
    }
}
