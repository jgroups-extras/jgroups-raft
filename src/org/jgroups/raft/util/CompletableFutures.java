package org.jgroups.raft.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Utility methods for operation related to {@link CompletableFuture} or {@link  CompletionStage}.
 */
// TODO move to jgroups!
public enum CompletableFutures {
   ;

   private static final Function<?, ?> NULL_FUNCTION = o -> null;

   public static <T> CompletionStage<T> completeExceptionally(Throwable throwable) {
      CompletableFuture<T> cf = new CompletableFuture<>();
      cf.completeExceptionally(throwable);
      return cf;
   }

   public static CompletionException wrapAsCompletionException(Throwable throwable) {
      return throwable instanceof CompletionException ?
            (CompletionException) throwable :
            new CompletionException(throwable);
   }

   public static <T> Function<T, Void> toVoidFunction() {
      //noinspection unchecked
      return (Function<T, Void>) NULL_FUNCTION;
   }

}
