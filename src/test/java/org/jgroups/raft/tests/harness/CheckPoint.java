package org.jgroups.raft.tests.harness;

import org.jgroups.util.CompletableFutures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Behaves more or less like a map of {@link java.util.concurrent.Semaphore}s.
 * <p>
 * One thread will wait for an event via {@code await(...)} or {@code awaitStrict(...)}, and one or more
 * other threads will trigger the event via {@code trigger(...)} or {@code triggerForever(...)}.
 * </p>
 *
 * @author Dan Berindei
 * @see <a href="https://github.com/infinispan/infinispan">Infinispan</a>
 */
public class CheckPoint {

   private static final Logger LOGGER = LogManager.getLogger(CheckPoint.class);
   public static final int INFINITE = 999999999;

   private final String id;
   private final Lock lock = new ReentrantLock();
   private final Condition unblockCondition = lock.newCondition();
   private final Map<String, EventStatus> events = new HashMap<>();

   public CheckPoint() {
      this.id = "";
   }

   public CheckPoint(String name) {
      this.id = "[" + name + "] ";
   }

   public void awaitStrict(String event, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      awaitStrict(event, 1, timeout, unit);
   }

   public void uncheckedAwaitStrict(String event, long timeout, TimeUnit unit) {
       try {
           awaitStrict(event, timeout, unit);
       } catch (InterruptedException | TimeoutException e) {
           throw new RuntimeException(e);
       }
   }

   public CompletionStage<Void> awaitStrictAsync(String event, long timeout, TimeUnit unit, Executor executor) {
      return CompletableFuture.runAsync(() -> {
         try {
            awaitStrict(event, 1, timeout, unit);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (TimeoutException e) {
            rethrowExceptionIfPresent(e);
         }
      }, executor);
   }

   public void awaitStrict(String event, int count, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      if (!await(event, count, timeout, unit)) {
         throw new TimeoutException(id + "Timed out waiting for event " + event);
      }
   }

   private static void rethrowExceptionIfPresent(Throwable t) {
      if (t != null) {
         throw asCompletionException(t);
      }
   }

   private static CompletionException asCompletionException(Throwable t) {
      if (t instanceof CompletionException) {
         return ((CompletionException) t);
      } else {
         return new CompletionException(t);
      }
   }

   private boolean await(String event, int count, long timeout, TimeUnit unit) throws InterruptedException {
      LOGGER.info("{}: Waiting for event {} * {}", id, event, count);
      lock.lock();
      try {
         EventStatus status = events.computeIfAbsent(event, k -> new EventStatus());
         long waitNanos = unit.toNanos(timeout);
         while (waitNanos > 0) {
            if (status.available >= count) {
               status.available -= count;
               break;
            }
            waitNanos = unblockCondition.awaitNanos(waitNanos);
         }

         if (waitNanos <= 0) {
            LOGGER.error("{}: Timed out waiting for event {} * {} (available = {}, total = {})",
                       id, event, count, status.available, status.total);
            // let the triggering thread know that we timed out
            status.available = -1;
            return false;
         }

         LOGGER.info("{}: Received event {} * {} (available = {}, total = {})", id, event, count, status.available, status.total);
         return true;
      } finally {
         lock.unlock();
      }
   }

   public String peek(long timeout, TimeUnit unit, String... expectedEvents) throws InterruptedException {
      LOGGER.info("{}: Waiting for any one of events {}", id, Arrays.toString(expectedEvents));
      String found = null;
      lock.lock();
      try {
         long waitNanos = unit.toNanos(timeout);
         while (waitNanos > 0) {
            for (String event : expectedEvents) {
               EventStatus status = events.get(event);
               if (status != null && status.available >= 1) {
                  found = event;
                  break;
               }
            }
            if (found != null)
               break;

            waitNanos = unblockCondition.awaitNanos(waitNanos);
         }

         if (waitNanos <= 0) {
            LOGGER.info("{}: Peek did not receive any of {}", id, Arrays.toString(expectedEvents));
            return null;
         }

         EventStatus status = events.get(found);
         LOGGER.info("{}: Received event {} (available = {}, total = {})", id, found, status.available, status.total);
         return found;
      } finally {
         lock.unlock();
      }
   }

   public CompletableFuture<Void> future(String event, long timeout, TimeUnit unit, Executor executor) {
      return future(event, 1, timeout, unit, executor);
   }

   public CompletableFuture<Void> future(String event, int count, long timeout, TimeUnit unit, Executor executor) {
      return future0(event, count)
              .orTimeout(timeout, unit)
              .thenRunAsync(() -> LOGGER.info("Received event {} * {}", event, count), executor);
   }

   public CompletableFuture<Void> future0(String event, int count) {
      LOGGER.info("{}: Waiting for event {} * {}", id, event, count);
      lock.lock();
      try {
         EventStatus status = events.computeIfAbsent(event, k -> new EventStatus());
         if (status.available >= count) {
            status.available -= count;
            return CompletableFutures.completedNull();
         }
         if (status.requests == null) {
            status.requests = new ArrayList<>();
         }
         CompletableFuture<Void> f = new CompletableFuture<>();
         status.requests.add(new Request(f, count));
         return f;
      } finally {
         lock.unlock();
      }
   }

   public void trigger(String event) {
      trigger(event, 1);
   }

   public void triggerForever(String event) {
      trigger(event, INFINITE);
   }

   public void trigger(String event, int count) {
      lock.lock();
      try {
         EventStatus status = events.get(event);
         if (status == null) {
            status = new EventStatus();
            events.put(event, status);
         } else if (status.available < 0) {
            throw new IllegalStateException(id + "Thread already timed out waiting for event " + event);
         }

         // If triggerForever is called more than once, it will cause an overflow and the waiters will fail.
         status.available = count != INFINITE ? status.available + count : INFINITE;
         status.total = count != INFINITE ? status.total + count : INFINITE;
         LOGGER.info("{}: Triggering event {} * {} (available = {}, total = {})", id, event, count,
                    status.available, status.total);
         unblockCondition.signalAll();
         if (status.requests != null) {
            if (count == INFINITE) {
               status.requests.forEach(request -> request.future.complete(null));
            } else {
               Iterator<Request> iterator = status.requests.iterator();
               while (status.available > 0 && iterator.hasNext()){
                  Request request = iterator.next();
                  if (request.count <= status.available) {
                     request.future.complete(null);
                     status.available -= request.count;
                     iterator.remove();
                  }
               }
            }
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public String toString() {
      return "CheckPoint(" + id + ")" + events;
   }

   private static class EventStatus {
      int available;
      int total;
      public ArrayList<Request> requests;

      @Override
      public String toString() {
         return available + "/" + total + ", requests=" + requests;
      }
   }

   private static class Request {
      final CompletableFuture<Void> future;
      final int count;

      private Request(CompletableFuture<Void> future, int count) {
         this.future = future;
         this.count = count;
      }

      @Override
      public String toString() {
         return "(" + count + ")";
      }
   }
}
