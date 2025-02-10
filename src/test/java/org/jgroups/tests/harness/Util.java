package org.jgroups.tests.harness;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Util {
    private Util() { }

    public static <T> T wait(CompletionStage<T> cs, long time, TimeUnit unit) {
        try {
            return cs.toCompletableFuture().get(time, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
