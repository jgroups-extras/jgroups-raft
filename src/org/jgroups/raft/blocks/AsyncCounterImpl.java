package org.jgroups.raft.blocks;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.util.AsciiString;
import org.jgroups.util.CompletableFutures;

/**
 * RAFT Implementation of {@link AsyncCounter}.
 *
 * @since 1.0.9
 */
public class AsyncCounterImpl implements AsyncCounter {

   private final CounterService raftCounterService;
   private final String name;
   private final AsciiString asciiName;
   private final Sync sync;

   public AsyncCounterImpl(CounterService raftCounterService, String name) {
      System.out.println(name);
      this.raftCounterService = raftCounterService;
      this.name = name;
      this.asciiName = new AsciiString(name);
      this.sync = new Sync();
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public CompletionStage<Long> get() {
      return raftCounterService.allowDirtyReads() ?
            CompletableFuture.completedFuture(raftCounterService._get(name)) :
            raftCounterService.asyncGet(asciiName);
   }

   @Override
   public CompletionStage<Void> set(long new_value) {
      return raftCounterService.asyncSet(asciiName, new_value);
   }

   @Override
   public CompletionStage<Long> compareAndSwap(long expect, long update) {
      return raftCounterService.asyncCompareAndSwap(asciiName, expect, update);
   }

   @Override
   public CompletionStage<Long> incrementAndGet() {
      return raftCounterService.asyncIncrementAndGet(asciiName);
   }

   @Override
   public CompletionStage<Long> decrementAndGet() {
      return raftCounterService.asyncDecrementAndGet(asciiName);
   }

   @Override
   public CompletionStage<Long> addAndGet(long delta) {
      return raftCounterService.asyncAddAndGet(asciiName, delta);
   }

   @Override
   public SyncCounter sync() {
      return sync;
   }

   private final class Sync implements SyncCounter {

      @Override
      public String getName() {
         return name;
      }

      @Override
      public long get() {
         return CompletableFutures.join(AsyncCounterImpl.this.get());
      }

      @Override
      public void set(long new_value) {
         CompletableFutures.join(AsyncCounterImpl.this.set(new_value));
      }

      @Override
      public long compareAndSwap(long expect, long update) {
         return CompletableFutures.join(AsyncCounterImpl.this.compareAndSwap(expect, update));
      }

      @Override
      public long addAndGet(long delta) {
         return CompletableFutures.join(AsyncCounterImpl.this.addAndGet(delta));
      }

      @Override
      public AsyncCounter async() {
         return AsyncCounterImpl.this;
      }
   }
}
